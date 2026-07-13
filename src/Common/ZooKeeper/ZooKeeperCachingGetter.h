#pragma once
#include <Common/ZooKeeper/Common.h>

#include <boost/noncopyable.hpp>

namespace zkutil
{

class ZooKeeperCachingGetter : boost::noncopyable
{
public:
    enum class SessionStatus : uint8_t
    {
        New,
        Cached
    };

    explicit ZooKeeperCachingGetter(zkutil::GetZooKeeper get_zookeeper_);

    ZooKeeperCachingGetter(const ZooKeeperCachingGetter &) = delete;
    ZooKeeperCachingGetter & operator=(const ZooKeeperCachingGetter &) = delete;

    /// Returns the ZooKeeper session and the status whether it was taken from the cache or opened new,
    /// because the session has expired or the cache was empty
    std::pair<zkutil::ZooKeeperPtr, SessionStatus> getZooKeeper();
    void resetCache();

private:
    std::pair<zkutil::ZooKeeperPtr, SessionStatus> getZooKeeperNoLock() TSA_REQUIRES(cached_zookeeper_ptr_mutex);

    std::mutex cached_zookeeper_ptr_mutex;
    zkutil::ZooKeeperPtr cached_zookeeper_ptr TSA_GUARDED_BY(cached_zookeeper_ptr_mutex);

    zkutil::GetZooKeeper get_zookeeper;
};

}
