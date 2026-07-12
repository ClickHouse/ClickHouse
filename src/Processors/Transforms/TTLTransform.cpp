#include <Processors/Transforms/TTLTransform.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/Context.h>

#include <Processors/Port.h>
#include <Processors/TTL/TTLAggregationAlgorithm.h>
#include <Processors/TTL/TTLColumnAlgorithm.h>
#include <Processors/TTL/TTLDeleteAlgorithm.h>
#include <Processors/TTL/TTLUpdateInfoAlgorithm.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/TTLResortUtils.h>

namespace DB
{

static TTLExpressions getExpressions(const TTLDescription & ttl_descr, PreparedSets::Subqueries & subqueries_for_sets, const ContextPtr & context)
{
    auto expr = ttl_descr.buildExpression(context);
    auto expr_queries = expr.sets->getSubqueries();
    subqueries_for_sets.insert(subqueries_for_sets.end(), expr_queries.begin(), expr_queries.end());

    auto where_expr = ttl_descr.buildWhereExpression(context);
    if (where_expr.sets)
    {
        auto where_expr_queries = where_expr.sets->getSubqueries();
        subqueries_for_sets.insert(subqueries_for_sets.end(), where_expr_queries.begin(), where_expr_queries.end());
    }

    return {expr.expression, where_expr.expression};
}

SharedHeader TTLTransform::addExpiredColumnsToBlock(const SharedHeader & header, const NamesAndTypesList & expired_columns_)
{
    if (expired_columns_.empty())
        return header;

    auto output_block = *header;

    for (const auto & col : expired_columns_)
    {
        if (output_block.has(col.name))
            continue;

        output_block.insert({col.type->createColumn(), col.type, col.name});
    }

    return std::make_shared<const Block>(std::move(output_block));
}

TTLTransform::TTLTransform(
    const ContextPtr & context,
    SharedHeader header_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeData::MutableDataPartPtr & data_part_,
    const NamesAndTypesList & expired_columns_,
    time_t current_time_,
    bool force_)
    : IAccumulatingTransform(header_, addExpiredColumnsToBlock(header_, expired_columns_))
    , data_part(data_part_)
    , expired_columns(expired_columns_)
    , log(getLogger(storage_.getLogName() + " (TTLTransform)"))
{
    auto old_ttl_infos = data_part->ttl_infos;

    if (metadata_snapshot_->hasRowsTTL())
    {
        const auto & rows_ttl = metadata_snapshot_->getRowsTTL();
        auto algorithm = std::make_unique<TTLDeleteAlgorithm>(
            getExpressions(rows_ttl, subqueries_for_sets, context), rows_ttl,
            old_ttl_infos.table_ttl, current_time_, force_);

        /// Skip all data if table ttl is expired for part
        if (algorithm->isMaxTTLExpired() && !rows_ttl.where_expression_ast)
            all_data_dropped = true;

        algorithms.emplace_back(std::move(algorithm));
        delete_algorithm = static_cast<const TTLDeleteAlgorithm *>(algorithms.back().get());
    }

    for (const auto & where_ttl : metadata_snapshot_->getRowsWhereTTLs())
        algorithms.emplace_back(std::make_unique<TTLDeleteAlgorithm>(
            getExpressions(where_ttl, subqueries_for_sets, context), where_ttl,
            old_ttl_infos.rows_where_ttl[where_ttl.result_column], current_time_, force_));

    /// Each GROUP BY TTL's TTLAggregationAlgorithm assumes its input is ordered by its own
    /// group_by_keys. The algorithms run sequentially on the same block, so if an EARLIER GROUP BY
    /// TTL's SET rewrites a column that a LATER TTL's group_by key derives from, the later
    /// algorithm's input is no longer ordered by that key and, with the streaming flush-on-key-change,
    /// it would fragment/lose groups. Worse, a derived key (a computed key such as `toStartOfDay(ts)`,
    /// a subcolumn key such as `t.a`, or a MATERIALIZED column used as a key) still holds its pre-SET
    /// value in the block, so the aggregation would even group by the stale key. Detect such a later
    /// TTL by mapping its group_by keys back to their physical storage columns and comparing with the
    /// earlier SET targets (a raw name comparison misses computed/subcolumn keys): tell its algorithm
    /// the input is unsorted (defer finalization to end of stream) AND refresh the derived key columns
    /// in the block before it runs.
    NameSet earlier_group_by_set_targets;
    bool earlier_group_by_lost_order = false;
    for (const auto & group_by_ttl : metadata_snapshot_->getGroupByTTLs())
    {
        const bool affected_by_earlier_set = groupByKeysAffectedByEarlierSet(
            group_by_ttl.group_by_keys, earlier_group_by_set_targets, metadata_snapshot_, context);

        /// Once ANY earlier GROUP BY TTL has to run unsorted, its accumulated state is flushed via
        /// `finalizeAggregates` -> `Aggregator::convertToChunks`, which iterates the hash table and does
        /// NOT preserve primary-key order. So the stream a later GROUP BY TTL then consumes is no longer
        /// ordered by ANY key -- not even a shorter, unaffected key prefix (e.g. `GROUP BY day` after an
        /// earlier `GROUP BY day, region ... SET region` went unsorted). The later TTL must therefore also
        /// run unsorted, otherwise its streaming flush-on-key-change would re-fragment the scrambled groups.
        const bool input_unsorted = affected_by_earlier_set || earlier_group_by_lost_order;

        ExpressionActionsPtr key_refresh_actions;
        /// Only THIS TTL's own key being rewritten by an earlier SET makes its in-stream key value stale
        /// and in need of refreshing. Losing input order (the cascade case above) does not change the key
        /// values, so no refresh is required there.
        if (affected_by_earlier_set)
        {
            if (auto refresh_dag = buildRefreshGroupByKeysDAG(
                    getInputPort().getHeader(), metadata_snapshot_, group_by_ttl.group_by_keys, context))
                key_refresh_actions = std::make_shared<ExpressionActions>(std::move(*refresh_dag));
        }

        algorithms.emplace_back(std::make_unique<TTLAggregationAlgorithm>(
                getExpressions(group_by_ttl, subqueries_for_sets, context), group_by_ttl,
                old_ttl_infos.group_by_ttl[group_by_ttl.result_column], current_time_, force_,
                getInputPort().getHeader(), storage_, /*input_sorted_by_group_by_keys=*/!input_unsorted));
        algorithm_key_refresh_actions.resize(algorithms.size());
        algorithm_key_refresh_actions.back() = std::move(key_refresh_actions);

        if (input_unsorted)
            earlier_group_by_lost_order = true;

        for (const auto & set_part : group_by_ttl.set_parts)
            earlier_group_by_set_targets.insert(set_part.column_name);
    }

    const auto & storage_columns = metadata_snapshot_->getColumns();
    const auto & column_defaults = storage_columns.getDefaults();

    auto build_default_expr = [&](const String & name)
    {
        using Result = std::pair<ExpressionActionsPtr, String>;
        auto it = column_defaults.find(name);
        if (it == column_defaults.end())
            return Result{};
        const auto & column = storage_columns.get(name);
        auto default_ast = it->second.expression->clone();
        default_ast = addTypeConversionToAST(std::move(default_ast), column.type->getName());
        auto syntax_result = TreeRewriter(storage_.getContext()).analyze(default_ast, storage_columns.getAll());
        auto actions = ExpressionAnalyzer{default_ast, syntax_result, storage_.getContext()}.getActions(true);
        return Result{actions, default_ast->getColumnName()};
    };

    for (const auto & expired_column : expired_columns)
    {
        auto [default_expression, default_column_name] = build_default_expr(expired_column.name);
        expired_columns_data.emplace(
            expired_column.name, ExpiredColumnData{expired_column.type, std::move(default_expression), std::move(default_column_name)});
    }

    if (metadata_snapshot_->hasAnyColumnTTL())
    {
        auto expired_columns_map = expired_columns.getNameToTypeMap();
        for (const auto & [name, description] : metadata_snapshot_->getColumnTTLs())
        {
            if (!expired_columns_map.contains(name))
            {
                auto [default_expression, default_column_name] = build_default_expr(name);
                algorithms.emplace_back(std::make_unique<TTLColumnAlgorithm>(
                    getExpressions(description, subqueries_for_sets, context),
                    description,
                    old_ttl_infos.columns_ttl[name],
                    current_time_,
                    force_,
                    name,
                    default_expression,
                    default_column_name,
                    isCompactPart(data_part)));
            }
        }
    }

    for (const auto & move_ttl : metadata_snapshot_->getMoveTTLs())
        algorithms.emplace_back(std::make_unique<TTLUpdateInfoAlgorithm>(
            getExpressions(move_ttl, subqueries_for_sets, context), move_ttl,
            TTLUpdateField::MOVES_TTL, move_ttl.result_column, old_ttl_infos.moves_ttl[move_ttl.result_column], current_time_, force_));

    for (const auto & recompression_ttl : metadata_snapshot_->getRecompressionTTLs())
        algorithms.emplace_back(std::make_unique<TTLUpdateInfoAlgorithm>(
            getExpressions(recompression_ttl, subqueries_for_sets, context), recompression_ttl,
            TTLUpdateField::RECOMPRESSION_TTL, recompression_ttl.result_column, old_ttl_infos.recompression_ttl[recompression_ttl.result_column], current_time_, force_));
}

static Block reorderColumns(Block block, const Block & header)
{
    Block res;
    for (const auto & col : header)
        res.insert(block.getByName(col.name));

    return res;
}

void TTLTransform::consume(Chunk chunk)
{
    if (all_data_dropped)
    {
        finishConsume();
        return;
    }

    removeSpecialColumnRepresentations(chunk);
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    /// Fill expired columns with default values which will later be handled in TTLColumnAlgorithm
    for (const auto & [column, data] : expired_columns_data)
    {
        auto default_column
            = ITTLAlgorithm::executeExpressionAndGetColumn(data.default_expression, block, data.default_column_name);
        if (default_column)
            default_column = default_column->convertToFullColumnIfConst();
        else
            default_column = data.type->createColumnConstWithDefaultValue(block.rows())->convertToFullColumnIfConst();

        /// Expired column may pre-exist (e.g. from customized merges like ReplacingMergeTree with version key), so
        /// replace it with default instead of inserting a new one.
        auto * c = block.findByName(column);
        if (c)
            c->column = default_column;
        else
            block.insert(ColumnWithTypeAndName(default_column, data.type, column));
    }

    for (size_t i = 0; i < algorithms.size(); ++i)
    {
        if (i < algorithm_key_refresh_actions.size() && algorithm_key_refresh_actions[i])
            algorithm_key_refresh_actions[i]->execute(block);
        algorithms[i]->execute(block);
    }

    if (block.empty())
        return;

    size_t num_rows = block.rows();
    setReadyChunk(Chunk(reorderColumns(std::move(block), getOutputPort().getHeader()).getColumns(), num_rows));
}

Chunk TTLTransform::generate()
{
    Block block;
    for (size_t i = 0; i < algorithms.size(); ++i)
    {
        /// On the end-of-stream flush, an earlier GROUP BY TTL finalizes its accumulated state into
        /// `block` (its last group). A later GROUP BY TTL then consumes that block, so if its
        /// group_by key was rewritten by the earlier SET, the key's derived column in the just-flushed
        /// rows is stale and must be refreshed here too (exactly as in `consume`). Only run the refresh
        /// once the block actually carries flushed rows; while it is still empty the algorithm only
        /// finalizes its own state and there is nothing to refresh.
        if (!block.empty() && i < algorithm_key_refresh_actions.size() && algorithm_key_refresh_actions[i])
            algorithm_key_refresh_actions[i]->execute(block);
        algorithms[i]->execute(block);
    }

    if (block.empty())
        return {};

    size_t num_rows = block.rows();
    return Chunk(reorderColumns(std::move(block), getOutputPort().getHeader()).getColumns(), num_rows);
}

void TTLTransform::finalize()
{
    data_part->ttl_infos = {};
    for (const auto & algorithm : algorithms)
        algorithm->finalize(data_part);

    if (delete_algorithm)
    {
        if (all_data_dropped)
            LOG_DEBUG(log, "Removed all rows from part {} due to expired TTL", data_part->name);
        else
            LOG_DEBUG(log, "Removed {} rows with expired TTL from part {}", delete_algorithm->getNumberOfRemovedRows(), data_part->name);
    }
    else
        LOG_DEBUG(log, "No delete algorithm was applied for part {}", data_part->name);
}

IProcessor::Status TTLTransform::prepare()
{
    auto status = IAccumulatingTransform::prepare();
    if (status == Status::Finished)
        finalize();

    return status;
}

}
