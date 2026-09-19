#include <Processors/Transforms/TTLTransform.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Interpreters/MaterializedColumnDependencies.h>
#include <Interpreters/Context.h>

#include <Processors/Port.h>
#include <Processors/TTL/TTLAggregationAlgorithm.h>
#include <Processors/TTL/TTLColumnAlgorithm.h>
#include <Processors/TTL/TTLDeleteAlgorithm.h>
#include <Processors/TTL/TTLUpdateInfoAlgorithm.h>

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

/// A column TTL resets its column to the default, but the `MATERIALIZED` columns computed from it keep
/// the value they had before the expiry, which contradicts their own expression forever - no later merge
/// repairs it. `ALTER TABLE ... CLEAR COLUMN`, the other path that resets a column to its default,
/// recomputes them, so do the same here, over the same dependency graph.
std::vector<std::vector<TTLTransform::DependentMaterializedColumn>> TTLTransform::analyzeDependentMaterializedColumns(
    const MergeTreeData & storage,
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & reset_columns,
    const NameSet & columns_to_leave_alone)
{
    const auto & storage_columns = metadata_snapshot->getColumns();
    const auto context = storage.getContext();

    /// The graph expands `ALIAS` references, reports a dependency on a subcolumn as one on the whole
    /// column and leaves out what reads an `EPHEMERAL` column, which no part holds. It also closes the
    /// set transitively: a dependent may read another dependent, whose recomputed value it has to see.
    MaterializedColumnDependencies materialized_dependencies(storage_columns, context);

    NameSet dependents;
    for (const auto & column : storage_columns)
        if (!materialized_dependencies.findColumnsToRecalculate(column.name, reset_columns).empty())
            dependents.insert(column.name);

    if (dependents.empty())
        return {};

    /// The level of a dependent is one past the highest level among the dependents it reads.
    std::unordered_map<String, size_t> level_of;
    auto level_of_column = [&](const String & name, auto && self) -> size_t
    {
        if (auto it = level_of.find(name); it != level_of.end())
            return it->second;

        /// The insertion doubles as an in-progress marker: `CREATE TABLE` rejects a cyclic default, but
        /// the marker is the memo entry itself, so making the recursion total costs nothing.
        level_of.emplace(name, 0);

        size_t level = 0;
        for (const auto & dependency : materialized_dependencies.findNode(name)->dependencies)
            if (dependency != name && dependents.contains(dependency))
                level = std::max(level, self(dependency, self) + 1);

        level_of[name] = level;
        return level;
    };

    size_t number_of_levels = 0;
    for (const auto & name : dependents)
        number_of_levels = std::max(number_of_levels, level_of_column(name, level_of_column) + 1);

    /// An expired column is already the default for the whole part, and recomputing a column that takes
    /// part in the sorting, the primary or the partition key would invalidate the order of the part being
    /// written. Such a column is left in `dependents` above: nothing changes it, so the dependents
    /// reading it get the value they already have, and dropping it here does not make them stale.
    NameSet unrecomputable_columns = columns_to_leave_alone;
    for (const auto & name : metadata_snapshot->getColumnsRequiredForSortingKey())
        unrecomputable_columns.insert(name);
    for (const auto & name : metadata_snapshot->getColumnsRequiredForPrimaryKey())
        unrecomputable_columns.insert(name);
    for (const auto & name : metadata_snapshot->getColumnsRequiredForPartitionKey())
        unrecomputable_columns.insert(name);

    std::vector<std::vector<DependentMaterializedColumn>> levels(number_of_levels);
    for (const auto & column : storage_columns)
    {
        if (!dependents.contains(column.name) || unrecomputable_columns.contains(column.name))
            continue;

        /// The graph keeps its expression pristine for every caller, and `analyze` rewrites in place.
        auto expression_ast
            = addTypeConversionToAST(materialized_dependencies.findNode(column.name)->expression->clone(), column.type->getName());
        auto syntax_result = TreeRewriter(context).analyze(expression_ast, storage_columns.getAllPhysical());
        auto expression = ExpressionAnalyzer{expression_ast, syntax_result, context}.getActions(true);

        /// A non-deterministic expression would also change the rows the TTL did not touch.
        if (expression->getActionsDAG().hasNonDeterministic())
            continue;

        levels[level_of[column.name]].push_back(
            {column.name, expression, expression_ast->getColumnName(), expression->getRequiredColumns()});
    }

    return levels;
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

    for (const auto & group_by_ttl : metadata_snapshot_->getGroupByTTLs())
        algorithms.emplace_back(std::make_unique<TTLAggregationAlgorithm>(
                getExpressions(group_by_ttl, subqueries_for_sets, context), group_by_ttl,
                old_ttl_infos.group_by_ttl[group_by_ttl.result_column], current_time_, force_,
                getInputPort().getHeader(), storage_));

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

    const auto column_ttls = metadata_snapshot_->getColumnTTLs();
    const auto expired_columns_map = expired_columns.getNameToTypeMap();

    /// The columns this transform resets to their default.
    NameSet reset_columns;
    for (const auto & [name, description] : column_ttls)
        reset_columns.insert(name);
    for (const auto & expired_column : expired_columns)
        reset_columns.insert(expired_column.name);

    std::vector<std::vector<DependentMaterializedColumn>> recompute_levels;
    NameSet recomputed_columns;
    if (!reset_columns.empty())
    {
        NameSet expired_column_names;
        for (const auto & expired_column : expired_columns)
            expired_column_names.insert(expired_column.name);

        recompute_levels
            = analyzeDependentMaterializedColumns(storage_, metadata_snapshot_, reset_columns, expired_column_names);

        for (const auto & level : recompute_levels)
            for (const auto & dependent : level)
                recomputed_columns.insert(dependent.name);
    }

    auto add_column_ttl_algorithm = [&](const String & name, const TTLDescription & description)
    {
        if (expired_columns_map.contains(name))
            return;

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
    };

    /// The TTL of a column that nothing recomputes goes first: a dependent has to read the value that
    /// TTL leaves behind, not the pre-expiry one.
    for (const auto & [name, description] : column_ttls)
        if (!recomputed_columns.contains(name))
            add_column_ttl_algorithm(name, description);

    /// Then the dependents, level by level. The own TTL of a dependent runs right after the level that
    /// recomputes it, so it wins row by row over the recomputed value: only the rows whose own TTL is
    /// due keep the default, the rest get the value their expression states.
    for (auto & level : recompute_levels)
    {
        if (level.empty())
            continue;

        recompute_stages.push_back({algorithms.size(), std::move(level)});

        for (const auto & dependent : recompute_stages.back().columns)
            if (auto it = column_ttls.find(dependent.name); it != column_ttls.end())
                add_column_ttl_algorithm(it->first, it->second);
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

void TTLTransform::recomputeDependentColumns(Block & block, const std::vector<DependentMaterializedColumn> & dependents)
{
    if (block.empty())
        return;

    /// The expressions read the post-TTL columns, so this restores the invariant the `MATERIALIZED`
    /// expression states. A mutation may read only a part of the columns, so a dependent whose inputs
    /// (or the dependent itself) are not in the block is left to the merge that has them.
    for (const auto & dependent : dependents)
    {
        if (!block.has(dependent.name))
            continue;

        if (std::any_of(
                dependent.required_columns.begin(),
                dependent.required_columns.end(),
                [&](const auto & required) { return !block.has(required); }))
            continue;

        auto recomputed = ITTLAlgorithm::executeExpressionAndGetColumn(dependent.expression, block, dependent.result_column_name);
        if (recomputed)
            block.getByName(dependent.name).column = recomputed->convertToFullColumnIfConst();
    }
}

/// `TTLAggregationAlgorithm` flushes the rows it accumulated from `generate`, and the column TTL
/// algorithms reset their columns in that block too, so both entrypoints go through here.
void TTLTransform::executeAlgorithms(Block & block)
{
    size_t next_stage = 0;
    for (size_t i = 0; i < algorithms.size(); ++i)
    {
        for (; next_stage < recompute_stages.size() && recompute_stages[next_stage].before_algorithm == i; ++next_stage)
            recomputeDependentColumns(block, recompute_stages[next_stage].columns);

        algorithms[i]->execute(block);
    }

    for (; next_stage < recompute_stages.size(); ++next_stage)
        recomputeDependentColumns(block, recompute_stages[next_stage].columns);
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

    executeAlgorithms(block);

    if (block.empty())
        return;

    size_t num_rows = block.rows();
    setReadyChunk(Chunk(reorderColumns(std::move(block), getOutputPort().getHeader()).getColumns(), num_rows));
}

Chunk TTLTransform::generate()
{
    Block block;
    executeAlgorithms(block);

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
