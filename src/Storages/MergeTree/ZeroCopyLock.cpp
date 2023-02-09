#include "ZeroCopyLock.h"

namespace DB
{
    ZeroCopyLock::ZeroCopyLock(const zkutil::ZooKeeperPtr & zookeeper, const std::string & lock_path)
        : lock(zkutil::createSimpleZooKeeperLock(zookeeper, lock_path, "part_exclusive_lock", ""))
    {
    }
}
