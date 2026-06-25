#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/StorageMergeTree.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

bool hasCommittingBlockInGap(
    const CommittingBlocksSet & committing_blocks, const String & partition_id, Int64 left_max_block, Int64 right_min_block)
{
    if (left_max_block + 1 >= right_min_block)
        return false;

    /// Block numbers are global across partitions, so scan committing blocks numerically inside the gap
    /// and match the partition. Only NewPart blocks become an active part that fills the gap;
    /// Mutation/Update blocks are version bumps that consume the same counter without creating a part.
    for (auto it = committing_blocks.upper_bound(left_max_block); it != committing_blocks.end() && it->number < right_min_block; ++it)
    {
        if (it->op == CommittingBlock::Op::NewPart && it->partition_id == partition_id)
            return true;
    }

    return false;
}

PlainCommittingBlockHolder::PlainCommittingBlockHolder(CommittingBlock block_, StorageMergeTree & storage_)
    : block(std::move(block_)), storage(storage_)
{
}

PlainCommittingBlockHolder::~PlainCommittingBlockHolder()
{
    storage.removeCommittingBlock(block);
}

template <class Enum>
int64_t toIntChecked(Enum value)
{
    int64_t underlying = magic_enum::enum_integer(value);
    auto checked = magic_enum::enum_cast<Enum>(underlying);

    if (!checked.has_value())
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown {} value {}", magic_enum::enum_type_name<Enum>(), underlying);

    return underlying;
}

template <class Enum>
Enum fromIntChecked(int64_t underlying)
{
    auto checked = magic_enum::enum_cast<Enum>(underlying);

    if (!checked.has_value())
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown {} value {}", magic_enum::enum_type_name<Enum>(), underlying);

    return checked.value();
}

void serializeCommittingBlockOpToBuffer(CommittingBlock::Op op, WriteBuffer & out)
{
    out << "operation: " << toIntChecked(op) << "\n";
}

CommittingBlock::Op deserializeCommittingBlockOpFromBuffer(ReadBuffer & in)
{
    int64_t op = 0;
    in >> "operation: " >> op >> "\n";
    return fromIntChecked<CommittingBlock::Op>(op);
}

std::string serializeCommittingBlockOpToString(CommittingBlock::Op op)
{
    WriteBufferFromOwnString out;
    serializeCommittingBlockOpToBuffer(op, out);
    return out.str();
}

CommittingBlock::Op deserializeCommittingBlockOpFromString(const std::string & representation)
{
    try
    {
        if (!representation.starts_with("operation"))
            return CommittingBlock::Op::Unknown;

        ReadBufferFromString in(representation);
        auto committing_block_op = deserializeCommittingBlockOpFromBuffer(in);

        assertEOF(in);
        return committing_block_op;
    }
    catch (const Exception &)
    {
        return CommittingBlock::Op::Unknown;
    }
}

}
