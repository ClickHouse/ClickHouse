#include <memory>
#include <mutex>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
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
#include <base/logger_useful.h>
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
ConcurrentHashJoin::ConcurrentHashJoin(ContextPtr context_, std::shared_ptr<TableJoin> table_join_, size_t slots_, const Block & left_sample_block, const Block & right_sample_block, bool any_take_last_row_)
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

    dispatch_datas = {std::make_shared<BlockDispatchControlData>(), std::make_shared<BlockDispatchControlData>()};
    const auto & onexpr = table_join->getClauses()[0];
    auto & left_dispatch_data = *dispatch_datas[0];
    std::tie(left_dispatch_data.hash_expression_actions, left_dispatch_data.hash_column_name) = buildHashExpressionAction(left_sample_block, onexpr.key_names_left);

    auto & right_dispatch_data = *dispatch_datas[1];
    std::tie(right_dispatch_data.hash_expression_actions, right_dispatch_data.hash_column_name) = buildHashExpressionAction(right_sample_block, onexpr.key_names_right);
}

bool ConcurrentHashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    auto & dispatch_data = *dispatch_datas[1];
    Blocks dispatched_blocks;
    Block cloned_block = block;
    dispatchBlock(dispatch_data, cloned_block, dispatched_blocks);

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
                hash_join->data->addJoinedBlock(dispatched_block, check_limits);

                hash_join->mutex.unlock();
                iter = pending_blocks.erase(iter);
            }
            else
            {
                iter++;
            }
        }
    }

    if (check_limits)
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    auto & dispatch_data = *dispatch_datas[0];
    Blocks dispatched_blocks;
    Block cloned_block = block;
    dispatchBlock(dispatch_data, cloned_block, dispatched_blocks);
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
        std::lock_guard lokc(hash_join->mutex);
        res += hash_join->data->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lokc(hash_join->mutex);
        res += hash_join->data->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lokc(hash_join->mutex);
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

std::pair<std::shared_ptr<ExpressionActions>, String> ConcurrentHashJoin::buildHashExpressionAction(const Block & block, const Strings & based_columns_names)
{
    Strings hash_columns_names;
    WriteBufferFromOwnString col_buf;
    for (size_t i = 0, sz = based_columns_names.size(); i < sz; ++i)
    {
        if (i)
            col_buf << ",";
        col_buf << based_columns_names[i];
    }
    WriteBufferFromOwnString write_buf;
    write_buf << "cityHash64(" << col_buf.str() << ") % " << slots;

    auto settings = context->getSettings();
    ParserExpressionList hash_expr_parser(true);
    ASTPtr func_ast = parseQuery(hash_expr_parser, write_buf.str(), "Parse Block hash expression", settings.max_query_size, settings.max_parser_depth);
    auto hash_column_name = func_ast->children[0]->getColumnName();

    DebugASTLog<false> visit_log;
    const auto & names_and_types = block.getNamesAndTypesList();
    ActionsDAGPtr actions = std::make_shared<ActionsDAG>(names_and_types);
    PreparedSets prepared_sets;
    SubqueriesForSets subqueries_for_sets;
    ActionsVisitor::Data visitor_data(
        context,
        SizeLimits{settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode},
        10,
        names_and_types,
        std::move(actions),
        prepared_sets,
        subqueries_for_sets,
        true, false, true, false);
    ActionsVisitor(visitor_data, visit_log.stream()).visit(func_ast);
    actions = visitor_data.getActions();
    return {std::make_shared<ExpressionActions>(actions), hash_column_name};
}

void ConcurrentHashJoin::dispatchBlock(BlockDispatchControlData & dispatch_data, Block & from_block, Blocks & dispatched_blocks)
{
    auto header = from_block.cloneEmpty();
    auto num_shards = hash_joins.size();
    Block block_for_build_selector = from_block;
    dispatch_data.hash_expression_actions->execute(block_for_build_selector);
    auto selector_column = block_for_build_selector.getByName(dispatch_data.hash_column_name);
    std::vector<UInt64> selector_slots;
    for (UInt64 i = 0; i < num_shards; ++i)
    {
        selector_slots.emplace_back(i);
        dispatched_blocks.emplace_back(from_block.cloneEmpty());
    }
    if (selector_column.column->isNullable())
    {
        // use the default value for null rows.
        selector_column.column = typeid_cast<const ColumnNullable *>(selector_column.column.get())->getNestedColumnPtr();
    }
    auto selector = createBlockSelector<UInt8>(*selector_column.column, selector_slots);

    auto columns_in_block = header.columns();
    for (size_t i = 0; i < columns_in_block; ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            dispatched_blocks[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }

}

}
}
