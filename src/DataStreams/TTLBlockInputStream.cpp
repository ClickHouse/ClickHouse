#include <DataStreams/TTLBlockInputStream.h>
#include <DataTypes/DataTypeDate.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Storages/TTLMode.h>
#include <Interpreters/Context.h>

#include <DataStreams/TTLDeleteAlgorithm.h>
#include <DataStreams/TTLColumnAlgorithm.h>
#include <DataStreams/TTLAggregationAlgorithm.h>
#include <DataStreams/TTLUpdateInfoAlgorithm.h>

namespace DB
{

TTLBlockInputStream::TTLBlockInputStream(
    const BlockInputStreamPtr & input_,
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeData::MutableDataPartPtr & data_part_,
    time_t current_time_,
    bool force_)
    : data_part(data_part_)
    , log(&Poco::Logger::get(storage_.getLogName() + " (TTLBlockInputStream)"))
{
    children.push_back(input_);
    header = children.at(0)->getHeader();
    auto old_ttl_infos = data_part->ttl_infos;

    if (metadata_snapshot_->hasRowsTTL())
    {
        const auto & rows_ttl = metadata_snapshot_->getRowsTTL();
        auto algorithm = std::make_unique<TTLDeleteAlgorithm>(
            rows_ttl, old_ttl_infos.table_ttl, current_time_, force_);

        /// Skip all data if table ttl is expired for part
        if (algorithm->isMaxTTLExpired() && !rows_ttl.where_expression)
            all_data_dropped = true;

        delete_algorithm = algorithm.get();
        algorithms.emplace_back(std::move(algorithm));
    }

    for (const auto & where_ttl : metadata_snapshot_->getRowsWhereTTLs())
        algorithms.emplace_back(std::make_unique<TTLDeleteAlgorithm>(
            where_ttl, old_ttl_infos.rows_where_ttl[where_ttl.result_column], current_time_, force_));

    for (const auto & group_by_ttl : metadata_snapshot_->getGroupByTTLs())
        algorithms.emplace_back(std::make_unique<TTLAggregationAlgorithm>(
            group_by_ttl, old_ttl_infos.group_by_ttl[group_by_ttl.result_column], current_time_, force_, header, storage_));

    if (metadata_snapshot_->hasAnyColumnTTL())
    {
        const auto & storage_columns = metadata_snapshot_->getColumns();
        const auto & column_defaults = storage_columns.getDefaults();

        for (const auto & [name, description] : metadata_snapshot_->getColumnTTLs())
        {
            ExpressionActionsPtr default_expression;
            String default_column_name;
            auto it = column_defaults.find(name);
            if (it != column_defaults.end())
            {
                const auto & column = storage_columns.get(name);
                auto default_ast = it->second.expression->clone();
                default_ast = addTypeConversionToAST(std::move(default_ast), column.type->getName());

                auto syntax_result
                    = TreeRewriter(storage_.getContext()).analyze(default_ast, metadata_snapshot_->getColumns().getAllPhysical());
                default_expression = ExpressionAnalyzer{default_ast, syntax_result, storage_.getContext()}.getActions(true);
                default_column_name = default_ast->getColumnName();
            }

            algorithms.emplace_back(std::make_unique<TTLColumnAlgorithm>(
                description, old_ttl_infos.columns_ttl[name], current_time_,
                force_, name, default_expression, default_column_name));
        }
    }

    for (const auto & move_ttl : metadata_snapshot_->getMoveTTLs())
        algorithms.emplace_back(std::make_unique<TTLMoveAlgorithm>(
            move_ttl, old_ttl_infos.moves_ttl[move_ttl.result_column], current_time_, force_));

    for (const auto & recompression_ttl : metadata_snapshot_->getRecompressionTTLs())
        algorithms.emplace_back(std::make_unique<TTLRecompressionAlgorithm>(
            recompression_ttl, old_ttl_infos.recompression_ttl[recompression_ttl.result_column], current_time_, force_));
}

Block reorderColumns(Block block, const Block & header)
{
    Block res;
    for (const auto & col : header)
        res.insert(block.getByName(col.name));

    return res;
}

Block TTLBlockInputStream::readImpl()
{
    if (all_data_dropped)
        return {};

    auto block = children.at(0)->read();
    for (const auto & algorithm : algorithms)
        algorithm->execute(block);

    if (!block)
        return block;

    return reorderColumns(std::move(block), header);
}

void TTLBlockInputStream::readSuffixImpl()
{
    data_part->ttl_infos = {};
    for (const auto & algorithm : algorithms)
        algorithm->finalize(data_part);

    if (delete_algorithm)
    {
        size_t rows_removed = all_data_dropped ? data_part->rows_count : delete_algorithm->getNumberOfRemovedRows();
        LOG_DEBUG(log, "Removed {} rows with expired TTL from part {}", rows_removed, data_part->name);
    }
}

}
