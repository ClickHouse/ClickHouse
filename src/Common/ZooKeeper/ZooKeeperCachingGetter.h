#pragma once
#include <Common/ZooKeeper/Common.h>


namespace zkutil
{

class ZooKeeperCachingGetter : boost::noncopyable
{
public:
    explicit ZooKeeperCachingGetter(zkutil::GetZooKeeper get_zookeeper_);

    ZooKeeperCachingGetter(const ZooKeeperCachingGetter &) = delete;
    ZooKeeperCachingGetter & operator=(const ZooKeeperCachingGetter &) = delete;

    /// Returns the ZooKeeper session and the flag whether it was taken from the cache(false) or opened new(true),
    /// because the session has expired or the cache was empty
    std::pair<zkutil::ZooKeeperPtr, bool> getZooKeeper();
    void resetCache();

private:
    std::pair<zkutil::ZooKeeperPtr, bool> getZooKeeperNoLock() TSA_REQUIRES(cached_zookeeper_ptr_mutex);

    std::mutex cached_zookeeper_ptr_mutex;
    zkutil::ZooKeeperPtr cached_zookeeper_ptr TSA_GUARDED_BY(cached_zookeeper_ptr_mutex);

    zkutil::GetZooKeeper get_zookeeper;
};

}
