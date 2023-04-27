#include "ConcurrentHashJoin.h"
#include <memory>
#include <mutex>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

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
    inner_join = std::make_unique<HashJoin>(table_join, right_sample_block);
    shared_context = std::make_shared<SharedContext>();
    shared_context->original_size_limit = table_join->sizeLimits();
    shared_context->size_limit_per_clone = table_join->sizeLimits();
    shared_context->clone_count = 1;
}

ConcurrentHashJoin::ConcurrentHashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, SharedContextPtr shared_context_)
    : table_join(table_join_)
    , right_sample_block(right_sample_block_)
{
    inner_join = std::make_unique<HashJoin>(table_join, right_sample_block);
    shared_context = shared_context_;
}

bool ConcurrentHashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    if (!inner_join->addJoinedBlock(block, false))
        return false;
    if (check_limits)
    {
        auto total_rows = getTotalRowCount();
        auto total_bytes = getTotalByteCount();
        return shared_context->size_limit_per_clone.check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
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
    const auto & res = inner_join->getTotals();
    return res;
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    return inner_join->getTotalRowCount();
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    return inner_join->getTotalByteCount();
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
    auto res = std::make_shared<ConcurrentHashJoin>(table_join, right_sample_block, shared_context);
    shared_context->clone_count += 1;
    shared_context->size_limit_per_clone.max_bytes = shared_context->original_size_limit.max_bytes / shared_context->clone_count;
    shared_context->size_limit_per_clone.max_rows = shared_context->original_size_limit.max_rows / shared_context->clone_count;
    return res;
}

bool ConcurrentHashJoin::isSupported(const std::shared_ptr<TableJoin> & table_join)
{
    if (table_join->strictness() == JoinStrictness::Asof)
        return false;
    if (!isInnerOrLeft(table_join->kind()) && !isRight(table_join->kind()))
    {
        return false;
    }
    if (table_join->isSpecialStorage() || !table_join->oneDisjunct())
        return false;
    return true;
}

}
