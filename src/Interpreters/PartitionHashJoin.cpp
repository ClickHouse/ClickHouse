#include "PartitionHashJoin.h"
#include <Interpreters/Context.h>
#include "Common/Exception.h"
#include "Interpreters/IJoin.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PartitionHashJoin::PartitionHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, JoinGetter getter)
    : context(context_)
    , table_join(table_join_)
    , join_getter(getter)
{
    init_join = join_getter();
}


bool PartitionHashJoin::addJoinedBlock(const Block &, bool)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::addJoinedBlock should not be called");
}
void PartitionHashJoin::checkTypesOfKeys(const Block & block) const
{
    // throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::checkTypesOfKeys should not be called");
    init_join->checkTypesOfKeys(block);
}

void PartitionHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed)
{
    // throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::joinBlock should not be called");
    init_join->joinBlock(block, not_processed);
}

void PartitionHashJoin::setTotals(const Block &)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::setTotals should not be called");

}
const Block & PartitionHashJoin::getTotals() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::getTotals should not be called");
}

size_t PartitionHashJoin::getTotalRowCount() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::getTotalRowCount should not be called");
}

size_t PartitionHashJoin::getTotalByteCount() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::getTotalByteCount should not be called");
}

bool PartitionHashJoin::alwaysReturnsEmptySet() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::alwaysReturnsEmptySet should not be called");
}

JoinProperty PartitionHashJoin::getJoinProperty() const
{
    return JoinProperty{
        .need_shuffle_partition_before = true,
        .has_inner_join = true,
    };
}

IBlocksStreamPtr PartitionHashJoin::getNonJoinedBlocks(const Block &, const Block &, UInt64) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PartitionHashJoin::getNonJoinedBlocks should not be called");
}

void PartitionHashJoin::setupInnerJoins(size_t n)
{
    if (!inner_joins.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "setupInnerJoins should be called once.");
    }
    for (size_t i = 0; i < n; ++i)
    {
        inner_joins.emplace_back(join_getter());
    }
}

JoinPtr PartitionHashJoin::getInnerJoin(size_t n)
{
    if (n >= inner_joins.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "n is overflow for inner_joins size");

    return inner_joins[n];
}
}
