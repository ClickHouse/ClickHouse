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

        delete_algorithm = algorithm.get();
        algorithms.emplace_back(std::move(algorithm));
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

Block reorderColumns(Block block, const Block & header)
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

    for (const auto & algorithm : algorithms)
        algorithm->execute(block);

    if (block.empty())
        return;

    size_t num_rows = block.rows();
    setReadyChunk(Chunk(reorderColumns(std::move(block), getOutputPort().getHeader()).getColumns(), num_rows));
}

Chunk TTLTransform::generate()
{
    Block block;
    for (const auto & algorithm : algorithms)
        algorithm->execute(block);

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
