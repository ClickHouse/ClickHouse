#include <Common/ZooKeeper/ZooKeeperCachingGetter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}

}

namespace zkutil
{

ZooKeeperCachingGetter::ZooKeeperCachingGetter(zkutil::GetZooKeeper get_zookeeper_) : get_zookeeper{get_zookeeper_}
{
}


void ZooKeeperCachingGetter::resetCache()
{
    std::lock_guard lock{cached_zookeeper_ptr_mutex};
    cached_zookeeper_ptr = nullptr;
}


std::pair<zkutil::ZooKeeperPtr, ZooKeeperCachingGetter::SessionStatus> ZooKeeperCachingGetter::getZooKeeper()
{
    std::lock_guard lock{cached_zookeeper_ptr_mutex};
    return getZooKeeperNoLock();
}


std::pair<zkutil::ZooKeeperPtr, ZooKeeperCachingGetter::SessionStatus> ZooKeeperCachingGetter::getZooKeeperNoLock()
{
    if (!cached_zookeeper_ptr || cached_zookeeper_ptr->expired())
    {
        auto zookeeper = get_zookeeper();
        if (!zookeeper)
            throw DB::Exception(DB::ErrorCodes::NO_ZOOKEEPER, "Can't get ZooKeeper session");

        cached_zookeeper_ptr = zookeeper;
        return {zookeeper, ZooKeeperCachingGetter::SessionStatus::New};
    }
    return {cached_zookeeper_ptr, ZooKeeperCachingGetter::SessionStatus::Cached};
}

}
