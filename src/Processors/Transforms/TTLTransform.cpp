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
    /// Whether a GROUP BY TTL can actually aggregate rows in THIS part, from its precomputed TTL info.
    /// The pessimizations below (marking a later TTL unsorted, cascading the lost-order flag) are only
    /// warranted when the earlier TTL that would rewrite/scramble the stream ACTUALLY fires: if no row
    /// in the part is expired for that TTL, `TTLAggregationAlgorithm::execute` aggregates nothing, its
    /// `SET` never runs, and the stream order and key values are left untouched. Deciding this from the
    /// mere presence of the clause in metadata would force `executeUnsorted` (whole-part external
    /// aggregation) even for a not-yet-expired earlier TTL. `group_by_ttl.min` is the minimum TTL value
    /// over the part's rows, so `min > current_time` means no row is expired and the TTL cannot fire.
    /// Conservative on the safe side: when the info is missing or uninitialized, assume it fires and keep
    /// the unsorted path -- we never take the streaming fast path on a scrambled stream. `force_` only
    /// makes the algorithm evaluate TTL expressions row by row; it does not make a future TTL expire.
    auto group_by_ttl_fires = [&](const TTLDescription & ttl) -> bool
    {
        auto it = old_ttl_infos.group_by_ttl.find(ttl.result_column);
        if (it == old_ttl_infos.group_by_ttl.end())
            return true;
        const auto min_ttl = it->second.min;
        return min_ttl == 0 || min_ttl <= current_time_;
    };

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

        /// `group_by_ttl_fires` reads the precomputed per-part `min`, which proves "won't fire" only for
        /// the UNMODIFIED part. If an earlier firing `GROUP BY ... SET` rewrote a column THIS TTL's expiry
        /// expression reads, that proof is void -- the earlier `SET` can move this TTL from future to
        /// expired in the same block, so it may aggregate/rewrite its own key here. Treat it as firing
        /// (conservative) so it propagates its `SET` targets and lost-order state to the NEXT TTL, keeping
        /// the streaming fast path off a stream a chained `SET` can scramble.
        /// The earlier SET can also invalidate THIS TTL's expiry: its expiry expression may read a
        /// MATERIALIZED column derived from a SET target (e.g. d MATERIALIZED toDate(ts2), expiry d + 1d,
        /// earlier SET ts2). The stored d is stale, so the algorithm would read the pre-SET value.
        const bool expiry_affected_by_earlier_set = groupByTTLExpiryAffectedByEarlierSet(
            group_by_ttl, earlier_group_by_set_targets, metadata_snapshot_, context);

        const bool set_expressions_affected_by_earlier_set = groupByTTLSetExpressionsAffectedByEarlierSet(
            group_by_ttl, earlier_group_by_set_targets, metadata_snapshot_, context);

        const bool this_ttl_fires = group_by_ttl_fires(group_by_ttl) || expiry_affected_by_earlier_set;

        ExpressionActionsPtr key_refresh_actions;
        /// Refresh the block's derived columns before this algorithm runs when an earlier SET made either
        /// THIS TTL's group_by key stale (aggregation would group by the pre-SET key) OR a MATERIALIZED
        /// column its expiry reads stale (isTTLExpired would read the pre-SET value and skip aggregation).
        /// Losing input order (the cascade case above) does not change any column value, so no refresh is
        /// needed there.
        if (affected_by_earlier_set || expiry_affected_by_earlier_set || set_expressions_affected_by_earlier_set)
        {
            if (auto refresh_dag = buildRefreshGroupByKeysDAG(
                    getInputPort().getHeader(), metadata_snapshot_, group_by_ttl, earlier_group_by_set_targets, context))
                key_refresh_actions = std::make_shared<ExpressionActions>(std::move(*refresh_dag));
        }

        algorithms.emplace_back(std::make_unique<TTLAggregationAlgorithm>(
                getExpressions(group_by_ttl, subqueries_for_sets, context), group_by_ttl,
                old_ttl_infos.group_by_ttl[group_by_ttl.result_column], current_time_, force_,
                getInputPort().getHeader(), storage_, /*input_sorted_by_group_by_keys=*/!input_unsorted));
        algorithm_key_refresh_actions.resize(algorithms.size());
        algorithm_key_refresh_actions.back() = std::move(key_refresh_actions);

        /// The stream is only actually scrambled for later TTLs when THIS TTL both runs unsorted AND
        /// fires (a firing unsorted TTL appends its aggregated groups in hash-table order at end of
        /// stream). A not-yet-expired unsorted TTL passes every row through in order, so it does not
        /// cost later TTLs their fast path.
        if (input_unsorted && this_ttl_fires)
            earlier_group_by_lost_order = true;

        /// Likewise, this TTL's `SET` only rewrites a later TTL's key when this TTL fires. Record its
        /// targets as "rewritten by an earlier SET" only then, so a future (not-yet-expired) earlier
        /// TTL does not needlessly force a later one off the streaming fast path.
        if (this_ttl_fires)
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

                /// An earlier firing `GROUP BY ... SET` can move a LATER column TTL from future to expired
                /// in the same block by rewriting a column its expiry reads -- directly, through a
                /// MATERIALIZED column (e.g. `d MATERIALIZED toDate(ts2)`, `payload TTL d + 1d`, earlier
                /// `SET ts2`), or through a pre-extracted subcolumn (e.g. `payload TTL tup.ts + 1d`, earlier
                /// `SET tup`). `TTLColumnAlgorithm` would otherwise trust its precomputed `min` and skip the
                /// column, and even when it runs it would read the stale derived expiry input. Detect the
                /// interaction with the SAME expiry check used for GROUP BY TTLs (it inspects the TTL's
                /// expiry columns; a column TTL has no group_by keys / WHERE), tell the algorithm to
                /// recompute expiry per row, and refresh the stale derived expiry inputs before it runs.
                const bool expiry_affected_by_earlier_set = groupByTTLExpiryAffectedByEarlierSet(
                    description, earlier_group_by_set_targets, metadata_snapshot_, context);

                ExpressionActionsPtr expiry_refresh_actions;
                if (expiry_affected_by_earlier_set)
                {
                    if (auto refresh_dag = buildRefreshGroupByKeysDAG(
                            getInputPort().getHeader(), metadata_snapshot_, description, earlier_group_by_set_targets, context))
                        expiry_refresh_actions = std::make_shared<ExpressionActions>(std::move(*refresh_dag));
                }

                algorithms.emplace_back(std::make_unique<TTLColumnAlgorithm>(
                    getExpressions(description, subqueries_for_sets, context),
                    description,
                    old_ttl_infos.columns_ttl[name],
                    current_time_,
                    force_,
                    name,
                    default_expression,
                    default_column_name,
                    isCompactPart(data_part),
                    /*earlier_set_can_expire=*/expiry_affected_by_earlier_set));
                algorithm_key_refresh_actions.resize(algorithms.size());
                algorithm_key_refresh_actions.back() = std::move(expiry_refresh_actions);
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
