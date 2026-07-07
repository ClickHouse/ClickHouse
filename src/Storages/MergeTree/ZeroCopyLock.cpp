#include <Storages/MergeTree/ZeroCopyLock.h>

namespace DB
{
    ZeroCopyLock::ZeroCopyLock(const zkutil::ZooKeeperPtr & zookeeper, const std::string & lock_path, const std::string & lock_message)
        : lock(zkutil::createSimpleZooKeeperLock(zookeeper, lock_path, ZERO_COPY_LOCK_NAME, lock_message))
    {
    }
}
