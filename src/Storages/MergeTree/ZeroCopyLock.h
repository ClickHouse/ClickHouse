#pragma once
#include <Core/Types.h>
#include <optional>
#include <memory>
#include <Common/ZooKeeper/ZooKeeperLock.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

/// Very simple wrapper for zookeeper ephemeral lock. It's better to have it
/// because due to bad abstraction we use it in MergeTreeData.
struct ZeroCopyLock
{
    ZeroCopyLock(const zkutil::ZooKeeperPtr & zookeeper, const std::string & lock_path);

    /// Actual lock
    std::unique_ptr<zkutil::ZooKeeperLock> lock;
};

}
