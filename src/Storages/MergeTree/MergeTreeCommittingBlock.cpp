#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/StorageMergeTree.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}

PlainCommittingBlockHolder::PlainCommittingBlockHolder(CommittingBlock block_, StorageMergeTree & storage_)
    : block(std::move(block_)), storage(storage_)
{
}

PlainCommittingBlockHolder::~PlainCommittingBlockHolder()
{
    storage.removeCommittingBlock(block);
}

CommittingBlocksSnapshot::CommittingBlocksSnapshot(const CommittingBlocksSet & blocks, Int64 last_allocated_block_)
    : min_update_block(getMinUpdateBlockNumber(blocks))
    , last_allocated_block(last_allocated_block_)
{
    for (const auto & block : blocks)
    {
        if (block.op == CommittingBlock::Op::Update || block.op == CommittingBlock::Op::Mutation)
            reservations.push_back(block);
    }
}

std::optional<CommittingBlock> CommittingBlocksSnapshot::firstReservationAfter(Int64 data_version) const
{
    auto it = std::ranges::upper_bound(reservations, data_version, std::less{}, &CommittingBlock::number);
    if (it == reservations.end())
        return std::nullopt;
    return *it;
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
