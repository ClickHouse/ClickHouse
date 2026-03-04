#pragma once

#include "ReplicatedMergeTreeMutationEntry.h"

#include <Common/ZooKeeper/ZooKeeper.h>

#include <optional>


namespace DB
{
class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

/// A class that is used for locking a block number in a partition.
/// Before 22.11 it used to create a secondary ephemeral node in `temp_path` with "abandonable_lock-" prefix
/// and a main ephemeral node with `path_prefix` that references the secondary node. The reasons for this two-level scheme are historical.
/// Since 22.11 it creates single ephemeral node with `path_prefix` that references persistent fake "secondary node".
class EphemeralLockInZooKeeper : public boost::noncopyable
{
    template<typename T>
    friend std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper(
        const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const T & deduplication_path,
        const std::optional<String> & znode_data);

protected:
    EphemeralLockInZooKeeper(const String & path_prefix_, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & path_, const String & conflict_path_ = "");

public:
    EphemeralLockInZooKeeper() = delete;

    /// Fake "secondary node" names for blocks with and without "deduplication_path"
    static constexpr const char * LEGACY_LOCK_INSERT = "abandonable_lock-insert";
    static constexpr const char * LEGACY_LOCK_OTHER = "abandonable_lock-other";
    static constexpr const char * LEGACY_LOCK_PREFIX = "/temp/abandonable_lock-";

    EphemeralLockInZooKeeper(EphemeralLockInZooKeeper && rhs) noexcept
    {
        *this = std::move(rhs);
    }

    EphemeralLockInZooKeeper & operator=(EphemeralLockInZooKeeper && rhs) noexcept
    {
        zookeeper = rhs.zookeeper;
        rhs.zookeeper = nullptr;
        path_prefix = std::move(rhs.path_prefix);
        path = std::move(rhs.path);
        conflict_path = std::move(rhs.conflict_path);
        return *this;
    }

    bool isLocked() const
    {
        return zookeeper.get();
    }

    String getPath() const
    {
        checkCreated();
        return path;
    }

    // In case of async inserts, we try to get locks for multiple inserts and need to know which insert is conflicted.
    // That's why we need this function.
    String getConflictPath() const
    {
        return conflict_path;
    }

    /// Parse the number at the end of the path.
    UInt64 getNumber() const;

    void unlock();

    /// Adds actions equivalent to `unlock()` to the list.
    /// Returns index of the action that removes
    void getUnlockOp(Coordination::Requests & ops);

    /// Do not delete nodes in destructor. You may call this method after 'getUnlockOps' and successful execution of these ops,
    ///  because the nodes will be already deleted.
    void assumeUnlocked()
    {
        zookeeper = nullptr;
    }

    void checkCreated() const;

    ~EphemeralLockInZooKeeper();

private:
    ZooKeeperWithFaultInjectionPtr zookeeper;
    String path_prefix;
    String path;
    String conflict_path;
};

template<typename T>
std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper(
    const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const T & deduplication_path,
    const std::optional<String> & znode_data);

/// Acquires block number locks in all partitions.
class EphemeralLocksInAllPartitions : public boost::noncopyable
{
public:
    EphemeralLocksInAllPartitions(
        const String & block_numbers_path, const String & path_prefix, const String & temp_path,
        zkutil::ZooKeeper & zookeeper_);

    EphemeralLocksInAllPartitions() = default;

    EphemeralLocksInAllPartitions(EphemeralLocksInAllPartitions && rhs) noexcept
        : zookeeper(rhs.zookeeper)
        , locks(std::move(rhs.locks))
    {
        rhs.zookeeper = nullptr;
    }

    EphemeralLocksInAllPartitions & operator=(EphemeralLocksInAllPartitions && rhs) noexcept
    {
        zookeeper = rhs.zookeeper;
        rhs.zookeeper = nullptr;
        locks = std::move(rhs.locks);
        return *this;
    }

    struct LockInfo
    {
        String path;

        String partition_id;
        UInt64 number = 0;
    };

    const std::vector<LockInfo> & getLocks() const { return locks; }

    void unlock();

    ~EphemeralLocksInAllPartitions();

private:
    zkutil::ZooKeeper * zookeeper = nullptr;
    std::vector<LockInfo> locks;
};


/// This class allows scoped manipulations with block numbers locked in certain partitions
/// See StorageReplicatedMergeTree::allocateBlockNumbersInAffectedPartitions and alter()/mutate() methods
class PartitionBlockNumbersHolder
{
public:
    PartitionBlockNumbersHolder(const PartitionBlockNumbersHolder &) = delete;
    PartitionBlockNumbersHolder & operator=(const PartitionBlockNumbersHolder &) = delete;

    using BlockNumbersType = ReplicatedMergeTreeMutationEntry::BlockNumbersType;

    PartitionBlockNumbersHolder() = default;
    PartitionBlockNumbersHolder(
        BlockNumbersType block_numbers_, std::optional<EphemeralLocksInAllPartitions> locked_block_numbers_holder)
        : block_numbers(std::move(block_numbers_))
        , multiple_partitions_holder(std::move(locked_block_numbers_holder))
    {
    }
    PartitionBlockNumbersHolder(
        BlockNumbersType block_numbers_, std::optional<EphemeralLockInZooKeeper> locked_block_numbers_holder)
        : block_numbers(std::move(block_numbers_))
        , single_partition_holder(std::move(locked_block_numbers_holder))
    {
    }

    PartitionBlockNumbersHolder & operator=(PartitionBlockNumbersHolder &&) = default;

    const BlockNumbersType & getBlockNumbers() const { return block_numbers; }

    void reset()
    {
        multiple_partitions_holder.reset();
        single_partition_holder.reset();
        block_numbers.clear();
    }

private:
    BlockNumbersType block_numbers;

    std::optional<EphemeralLocksInAllPartitions> multiple_partitions_holder;
    std::optional<EphemeralLockInZooKeeper> single_partition_holder;
};

}
