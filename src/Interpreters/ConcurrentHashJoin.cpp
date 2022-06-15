#include <memory>
#include <mutex>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SubqueryForSet.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/WeakHash.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
}

ConcurrentHashJoin::ConcurrentHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & right_sample_block, bool any_take_last_row_)
    : context(context_)
    , table_join(table_join_)
    , slots(slots_)
{
    if (slots < 1 || 255 < slots)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of slots should be [1, 255], got {}", slots);
    }

    for (size_t i = 0; i < slots; ++i)
    {
        auto inner_hash_join = std::make_shared<InternalHashJoin>();
        inner_hash_join->data = std::make_unique<HashJoin>(table_join_, right_sample_block, any_take_last_row_);
        hash_joins.emplace_back(std::move(inner_hash_join));
    }
}

bool ConcurrentHashJoin::addJoinedBlock(const Block & right_block, bool check_limits)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_right, right_block);

    size_t blocks_left = 0;
    for (const auto & block : dispatched_blocks)
    {
        if (block)
        {
            ++blocks_left;
        }
    }

    while (blocks_left > 0)
    {
        /// insert blocks into corresponding HashJoin instances
        for (size_t i = 0; i < dispatched_blocks.size(); ++i)
        {
            auto & hash_join = hash_joins[i];
            auto & dispatched_block = dispatched_blocks[i];

            if (dispatched_block)
            {
                /// if current hash_join is already processed by another thread, skip it and try later
                std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                if (!lock.owns_lock())
                    continue;

                bool limit_exceeded = !hash_join->data->addJoinedBlock(dispatched_block, check_limits);

                dispatched_block = {};
                blocks_left--;

                if (limit_exceeded)
                    return false;
            }
        }
    }

    if (check_limits)
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_left, block);
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        std::shared_ptr<ExtraBlock> none_extra_block;
        auto & hash_join = hash_joins[i];
        auto & dispatched_block = dispatched_blocks[i];
        hash_join->data->joinBlock(dispatched_block, none_extra_block);
        if (none_extra_block && !none_extra_block->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not_processed should be empty");
    }

    block = concatenateBlocks(dispatched_blocks);
}

void ConcurrentHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_joins[0]->data->checkTypesOfKeys(block);
}

void ConcurrentHashJoin::setTotals(const Block & block)
{
    if (block)
    {
        std::lock_guard lock(totals_mutex);
        totals = block;
    }
}

const Block & ConcurrentHashJoin::getTotals() const
{
    return totals;
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        if (!hash_join->data->alwaysReturnsEmptySet())
            return false;
    }
    return true;
}

std::shared_ptr<NotJoinedBlocks> ConcurrentHashJoin::getNonJoinedBlocks(
        const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return {};

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid join type. join kind: {}, strictness: {}", table_join->kind(), table_join->strictness());
}

Blocks ConcurrentHashJoin::dispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    size_t num_shards = hash_joins.size();
    return JoinCommon::scatterBlockByHash(key_columns_names, from_block, num_shards);
}

}
