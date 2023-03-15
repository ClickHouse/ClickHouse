#include "PartitionHashJoin.h"
#include <memory>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PartitionHashJoin::PartitionHashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
    : table_join(table_join_)
    , right_sample_block(right_sample_block_)
{
    inner_join = std::make_unique<HashJoin>(table_join, right_sample_block);
}


bool PartitionHashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    return inner_join->addJoinedBlock(block, check_limits);
}

void PartitionHashJoin::checkTypesOfKeys(const Block & block) const
{
    inner_join->checkTypesOfKeys(block);
}

void PartitionHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed)
{
    inner_join->joinBlock(block, not_processed);
}

void PartitionHashJoin::setTotals(const Block & block)
{
    inner_join->setTotals(block);
}
const Block & PartitionHashJoin::getTotals() const
{
    return inner_join->getTotals();
}

size_t PartitionHashJoin::getTotalRowCount() const
{
    return inner_join->getTotalRowCount();
}

size_t PartitionHashJoin::getTotalByteCount() const
{
    return inner_join->getTotalByteCount();
}

bool PartitionHashJoin::alwaysReturnsEmptySet() const
{
    return inner_join->alwaysReturnsEmptySet();
}


IBlocksStreamPtr
PartitionHashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    return inner_join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
}

JoinPtr PartitionHashJoin::clone() const
{
    return std::make_shared<PartitionHashJoin>(table_join, right_sample_block);
}

bool PartitionHashJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    if (table_join->strictness() == JoinStrictness::Asof)
        return false;
    if (table_join->isSpecialStorage() || !table_join->oneDisjunct())
        return false;
    return true;
}

}
