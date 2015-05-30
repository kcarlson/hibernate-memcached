/* Copyright 2015, original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.googlecode.hibernate.memcached.strategy;

import com.googlecode.hibernate.memcached.region.MemcachedEntityRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.cfg.Settings;

/**
 * Based on Ehcache specific non-strict read/write entity region access strategy.
 */
public class NonStrictReadWriteMemcachedEntityRegionAccessStrategy extends AbstractEntityRegionAccessStrategy {

    /**
     * Create a non-strict read/write access strategy accessing the given collection region.
     *
     * @param region   The wrapped region
     * @param settings The Hibernate settings
     */
    public NonStrictReadWriteMemcachedEntityRegionAccessStrategy(MemcachedEntityRegion region, Settings settings) {
        super(region, settings);
    }

    public Object get(Object key, long txTimestamp) throws CacheException {
        return region.getCache().get(key);
    }

    @Override
    public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride)
            throws CacheException {
        if (minimalPutOverride && region.contains(key)) {
            return false;
        } else {
            region.getCache().put(key, value);
            return true;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Since this is a non-strict read/write strategy item locking is not used.
     */
    public SoftLock lockItem(Object key, Object version) throws CacheException {
        return null;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Since this is a non-strict read/write strategy item locking is not used.
     */
    public void unlockItem(Object key, SoftLock lock) throws CacheException {
        region.getCache().remove(key);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Returns <code>false</code> since this is an asynchronous cache access strategy.
     */
    public boolean insert(Object key, Object value, Object version) throws CacheException {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Returns <code>false</code> since this is a non-strict read/write cache access strategy
     */
    public boolean afterInsert(Object key, Object value, Object version) throws CacheException {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Removes the entry since this is a non-strict read/write cache strategy.
     */
    public boolean update(Object key, Object value, Object currentVersion, Object previousVersion)
            throws CacheException {
        remove(key);
        return false;
    }

    public boolean afterUpdate(Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock)
            throws CacheException {
        unlockItem(key, lock);
        return false;
    }

    @Override
    public void remove(Object key) throws CacheException {
        region.getCache().remove(key);
    }
}
