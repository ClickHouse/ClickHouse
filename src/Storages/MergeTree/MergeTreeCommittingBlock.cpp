#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

PlainCommittingBlockHolder::PlainCommittingBlockHolder(CommittingBlock block_, StorageMergeTree & storage_)
    : block(std::move(block_)), storage(storage_)
{
}

PlainCommittingBlockHolder::~PlainCommittingBlockHolder()
{
    storage.removeCommittingBlock(block);
}

}
