#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** The synchronization is primitive. Works as follows:
  * Creates a non-ephemeral incremental node and marks it as locked (LOCKED).
  * `unlock()` unlocks it (UNLOCKED).
  * When the destructor is called or the session ends in ZooKeeper, it goes into the ABANDONED state.
  * (Including when the program is halted).
  */
class AbandonableLockInZooKeeper : public boost::noncopyable
{
public:
    enum State
    {
        UNLOCKED,
        LOCKED,
        ABANDONED,
    };

    AbandonableLockInZooKeeper(
        const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_, zkutil::Requests * precheck_ops = nullptr);

    AbandonableLockInZooKeeper() = default;

    AbandonableLockInZooKeeper(AbandonableLockInZooKeeper && rhs) noexcept
    {
        *this = std::move(rhs);
    }

    AbandonableLockInZooKeeper & operator=(AbandonableLockInZooKeeper && rhs) noexcept
    {
        zookeeper = rhs.zookeeper;
        rhs.zookeeper = nullptr;
        path_prefix = std::move(rhs.path_prefix);
        path = std::move(rhs.path);
        holder_path = std::move(rhs.holder_path);
        return *this;
    }

    bool isCreated() const
    {
        return zookeeper && !holder_path.empty() && !path.empty();
    }

    String getPath() const
    {
        checkCreated();
        return path;
    }

    /// Parse the number at the end of the path.
    UInt64 getNumber() const
    {
        checkCreated();
        return parse<UInt64>(path.c_str() + path_prefix.size(), path.size() - path_prefix.size());
    }

    void unlock();

    /// Adds actions equivalent to `unlock()` to the list.
    void getUnlockOps(zkutil::Requests & ops);

    /// Do not delete nodes in destructor. You may call this method after 'getUnlockOps' and successful execution of these ops,
    ///  because the nodes will be already deleted.
    void assumeUnlocked()
    {
        holder_path.clear();
    }

    void checkCreated() const
    {
        if (!isCreated())
            throw Exception("AbandonableLock is not created", ErrorCodes::LOGICAL_ERROR);
    }

    ~AbandonableLockInZooKeeper();

private:
    zkutil::ZooKeeper * zookeeper = nullptr;
    String path_prefix;
    String path;
    String holder_path;
};


/// Acquires block number locks in all partitions. The class is called Ephemeral- instead of Abandonable-
/// because it creates ephemeral block nodes (there is no need to leave abandoned tombstones).
class EphemeralLocksInAllPartitions
{
public:
    EphemeralLocksInAllPartitions(
        const String & block_numbers_path, const String & path_prefix, const String & temp_path,
        zkutil::ZooKeeper & zookeeper_);

    struct LockInfo
    {
        String path;
        String holder_path;

        String partition_id;
        UInt64 number = 0;
    };

    const std::vector<LockInfo> & getLocks() const { return locks; }

    void unlock();

    ~EphemeralLocksInAllPartitions();

private:
    zkutil::ZooKeeper & zookeeper;
    std::vector<LockInfo> locks;
};

}
