#pragma once

#include "ReplicatedMergeTreeMutationEntry.h"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>

#include <map>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// A class that is used for locking a block number in a partition.
/// It creates a secondary ephemeral node in `temp_path` and a main ephemeral node with `path_prefix`
/// that references the secondary node. The reasons for this two-level scheme are historical (of course
/// it would be simpler to allocate block numbers for all partitions in one ZK directory).
class EphemeralLockInZooKeeper : public boost::noncopyable
{
public:
    EphemeralLockInZooKeeper(
        const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_, Coordination::Requests * precheck_ops = nullptr);

    EphemeralLockInZooKeeper() = default;

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
    void getUnlockOps(Coordination::Requests & ops);

    /// Do not delete nodes in destructor. You may call this method after 'getUnlockOps' and successful execution of these ops,
    ///  because the nodes will be already deleted.
    void assumeUnlocked()
    {
        holder_path.clear();
    }

    void checkCreated() const
    {
        if (!isCreated())
            throw Exception("EphemeralLock is not created", ErrorCodes::LOGICAL_ERROR);
    }

    ~EphemeralLockInZooKeeper();

private:
    zkutil::ZooKeeper * zookeeper = nullptr;
    String path_prefix;
    String path;
    String holder_path;
};


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
        String holder_path;

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
