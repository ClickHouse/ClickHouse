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

struct PlainCommittingBlockHolder
{
    CommittingBlock block;
    StorageMergeTree & storage;

    PlainCommittingBlockHolder(CommittingBlock block_, StorageMergeTree & storage_);
    ~PlainCommittingBlockHolder();
};

std::string serializeCommittingBlockOpToString(CommittingBlock::Op op);
CommittingBlock::Op deserializeCommittingBlockOpFromString(const std::string & representation);

}
