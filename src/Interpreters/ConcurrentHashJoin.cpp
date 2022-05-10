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
#include <Common/typeid_cast.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int BAD_ARGUMENTS;
}
namespace JoinStuff
{
ConcurrentHashJoin::ConcurrentHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & right_sample_block, bool any_take_last_row_)
    : context(context_)
    , table_join(table_join_)
    , slots(slots_)
{
    if (!slots_ || slots_ >= 256)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid argument slot : {}", slots_);
    }

    for (size_t i = 0; i < slots; ++i)
    {
        auto inner_hash_join = std::make_shared<InternalHashJoin>();
        inner_hash_join->data = std::make_unique<HashJoin>(table_join_, right_sample_block, any_take_last_row_);
        hash_joins.emplace_back(std::move(inner_hash_join));
    }

}

bool ConcurrentHashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_right, block);

    std::list<size_t> pending_blocks;
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
        pending_blocks.emplace_back(i);
    while (!pending_blocks.empty())
    {
        for (auto iter = pending_blocks.begin(); iter != pending_blocks.end();)
        {
            auto & i = *iter;
            auto & hash_join = hash_joins[i];
            auto & dispatched_block = dispatched_blocks[i];
            if (hash_join->mutex.try_lock())
            {
                if (!hash_join->data->addJoinedBlock(dispatched_block, check_limits))
                {
                    hash_join->mutex.unlock();
                    return false;
                }

                hash_join->mutex.unlock();
                iter = pending_blocks.erase(iter);
            }
            else
                iter++;
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
    if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi ||
        !isRightOrFull(table_join->kind()))
    {
        return {};
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid join type. join kind: {}, strictness: {}", table_join->kind(), table_join->strictness());
}

Blocks ConcurrentHashJoin::dispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    Blocks result;

    size_t num_shards = hash_joins.size();
    size_t num_rows = from_block.rows();
    size_t num_cols = from_block.columns();

    ColumnRawPtrs key_cols;
    for (const auto & key_name : key_columns_names)
    {
        key_cols.push_back(from_block.getByName(key_name).column.get());
    }
    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        SipHash hash;
        for (const auto & key_col : key_cols)
        {
            key_col->updateHashWithValue(i, hash);
        }
        selector[i] = hash.get64() % num_shards;
    }

    for (size_t i = 0; i < num_shards; ++i)
    {
        result.emplace_back(from_block.cloneEmpty());
    }

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
}

}
}
