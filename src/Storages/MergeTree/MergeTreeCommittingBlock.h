#pragma once
#include <Core/Types.h>
#include <optional>
#include <set>
#include <vector>

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

    CommittingBlock() = default;
    CommittingBlock(Op op_, Int64 number_) : op(op_), number(number_) {}

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

/// Committing blocks and the block counter copied together: every later block is above `last_allocated_block`. A reservation
/// is an `Update` or `Mutation` block (a version allocated but not visible yet), never a `NewPart` block.
class CommittingBlocksSnapshot
{
public:
    CommittingBlocksSnapshot(const CommittingBlocksSet & blocks, Int64 last_allocated_block_);

    /// Lowest reservation strictly above `data_version`, or nullopt. The lookup is table-wide: an `Update`
    /// reservation bounds every partition, the same scope as the `min_update_block` rule.
    std::optional<CommittingBlock> firstReservationAfter(Int64 data_version) const;

    std::optional<Int64> minUpdateBlock() const { return min_update_block; }

    Int64 lastAllocatedBlock() const { return last_allocated_block; }

private:
    /// Sorted by number.
    std::vector<CommittingBlock> reservations;
    std::optional<Int64> min_update_block;
    Int64 last_allocated_block;
};

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
