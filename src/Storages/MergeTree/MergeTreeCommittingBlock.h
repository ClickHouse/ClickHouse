#pragma once
#include <Core/Types.h>
#include <set>

namespace DB
{

class StorageMergeTree;

struct CommittingBlock
{
    enum class Op : uint64_t
    {
        Unknown,
        NewPart,
        Update,
        Mutation,
    };

    Op op{Op::Unknown};
    Int64 number{};
    /// Partition of the part this block will become (set for NewPart). Plain MergeTree block numbers
    /// are global across partitions, so the gap check needs the partition to avoid cross-partition matches.
    String partition_id{};

    CommittingBlock() = default;
    CommittingBlock(Op op_, Int64 number_) : op(op_), number(number_) {}
    CommittingBlock(Op op_, Int64 number_, String partition_id_) : op(op_), number(number_), partition_id(std::move(partition_id_)) {}

    bool operator==(const CommittingBlock & other) const = default;
};

struct LessCommittingBlock
{
    using is_transparent = void;

    bool operator()(const CommittingBlock & lhs, Int64 rhs) const { return lhs.number < rhs; }
    bool operator()(Int64 lhs, const CommittingBlock & rhs) const { return lhs < rhs.number; }
    bool operator()(const CommittingBlock & lhs, const CommittingBlock & rhs) const { return lhs.number < rhs.number; }
};

using CommittingBlocksSet = std::set<CommittingBlock, LessCommittingBlock>;

/// True if a NewPart block for the given partition with left_max_block < n < right_min_block is still
/// committing, i.e. about to become an Active part strictly inside the gap between two parts being
/// considered for a merge. Block numbers are global across partitions, hence the partition filter.
bool hasCommittingBlockInGap(
    const CommittingBlocksSet & committing_blocks, const String & partition_id, Int64 left_max_block, Int64 right_min_block);

struct PlainCommittingBlockHolder
{
    CommittingBlock block;
    StorageMergeTree & storage;

    PlainCommittingBlockHolder(CommittingBlock block_, StorageMergeTree & storage_);
    ~PlainCommittingBlockHolder();
};

class ReadBuffer;
class WriteBuffer;

void serializeCommittingBlockOpToBuffer(CommittingBlock::Op op, WriteBuffer & out);
CommittingBlock::Op deserializeCommittingBlockOpFromBuffer(ReadBuffer & in);

std::string serializeCommittingBlockOpToString(CommittingBlock::Op op);
CommittingBlock::Op deserializeCommittingBlockOpFromString(const std::string & representation);

}
