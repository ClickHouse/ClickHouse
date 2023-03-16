#include "ConcurrentHashJoin.h"
#include <memory>
#include <mutex>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

ConcurrentHashJoin::ConcurrentHashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_)
    : table_join(table_join_)
    , right_sample_block(right_sample_block_)
{
    inner_join = std::make_shared<HashJoin>(table_join, right_sample_block);
    clone_joins = std::make_shared<CloneJoins>();
    clone_joins->clones.push_back(inner_join);
}

ConcurrentHashJoin::ConcurrentHashJoin(
    std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, std::shared_ptr<CloneJoins> clones_)
    : table_join(table_join_), right_sample_block(right_sample_block_), clone_joins(clones_)
{
    inner_join = std::make_unique<HashJoin>(table_join, right_sample_block);
}


bool ConcurrentHashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    if (!inner_join->addJoinedBlock(block, check_limits))
        return false;
    if (check_limits)
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::checkTypesOfKeys(const Block & block) const
{
    inner_join->checkTypesOfKeys(block);
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed)
{
    inner_join->joinBlock(block, not_processed);
}

void ConcurrentHashJoin::setTotals(const Block & block)
{
    inner_join->setTotals(block);
}
const Block & ConcurrentHashJoin::getTotals() const
{
    return inner_join->getTotals();
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    std::lock_guard lock(clone_joins->mutex);
    size_t res = 0;
    for (const auto & join : clone_joins->clones)
    {
        res += join->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    std::lock_guard lock(clone_joins->mutex);
    size_t res = 0;
    for (const auto & join : clone_joins->clones)
    {
        res += join->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    return inner_join->alwaysReturnsEmptySet();
}


IBlocksStreamPtr
ConcurrentHashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    return inner_join->getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
}

JoinPtr ConcurrentHashJoin::clone()
{
    auto res = std::make_shared<ConcurrentHashJoin>(table_join, right_sample_block, clone_joins);
    std::lock_guard lock(clone_joins->mutex);
    clone_joins->clones.push_back(res->inner_join);
    return res;
}

bool ConcurrentHashJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    if (table_join->strictness() == JoinStrictness::Asof)
        return false;
    if (!isInnerOrLeft(table_join->kind()) && !isRight(table_join->kind()))
        return false;
    if (table_join->isSpecialStorage() || !table_join->oneDisjunct())
        return false;
    return true;
}

}
