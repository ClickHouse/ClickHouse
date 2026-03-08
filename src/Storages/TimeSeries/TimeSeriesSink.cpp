#include <Storages/TimeSeries/TimeSeriesSink.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/addMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sources/BlocksSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace
{
    /// Calculates the identifier of each time series in "tags_block" using the default expression for the "id" column,
    /// and adds column "id" with the results to "tags_block".
    /// This follows the same pattern as PrometheusRemoteWriteProtocol::calculateId().
    IColumn & calculateId(const ContextPtr & context, const ColumnDescription & id_column_description, Block & tags_block)
    {
        auto blocks = std::make_shared<Blocks>();
        blocks->push_back(tags_block);

        auto header = std::make_shared<const Block>(tags_block.cloneEmpty());
        auto pipe = Pipe(std::make_shared<BlocksSource>(blocks, header));

        Block header_with_id;
        const auto & id_name = id_column_description.name;
        auto id_type = id_column_description.type;
        header_with_id.insert(ColumnWithTypeAndName{id_type, id_name});

        auto adding_missing_defaults_dag = addMissingDefaults(
            pipe.getHeader(),
            header_with_id.getNamesAndTypesList(),
            ColumnsDescription{id_column_description},
            context);

        auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(std::move(adding_missing_defaults_dag));
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, adding_missing_defaults_actions);
        });

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            header_with_id.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            context);
        auto actions = std::make_shared<ExpressionActions>(
            std::move(convert_actions_dag),
            ExpressionActionsSettings(context, CompileExpressions::yes));
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, actions);
        });

        QueryPipeline pipeline{std::move(pipe)};
        PullingPipelineExecutor executor{pipeline};

        MutableColumnPtr id_column;

        Block block_from_executor;
        while (executor.pull(block_from_executor))
        {
            if (!block_from_executor.empty())
            {
                MutableColumnPtr id_column_part = block_from_executor.getByName(id_name).column->assumeMutable();
                if (id_column)
                    id_column->insertRangeFrom(*id_column_part, 0, id_column_part->size());
                else
                    id_column = std::move(id_column_part);
            }
        }

        if (!id_column)
            id_column = id_type->createColumn();

        IColumn & id_column_ref = *id_column;
        tags_block.insert(0, ColumnWithTypeAndName{std::move(id_column), id_type, id_name});
        return id_column_ref;
    }
}


TimeSeriesSink::TimeSeriesSink(
    StorageTimeSeries & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , WithContext(context_)
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , time_series_settings(std::make_shared<TimeSeriesSettings>(storage_.getStorageSettings()))
    , log(getLogger("TimeSeriesSink"))
{
}

void TimeSeriesSink::consume(Chunk & chunk)
{
    size_t rows = chunk.getNumRows();
    if (!rows)
        return;

    auto block = getHeader().cloneWithColumns(chunk.getColumns());
    splitBlock(block);
}

void TimeSeriesSink::splitBlock(const Block & block)
{
    const auto & columns_description = metadata_snapshot->columns;
    const size_t num_rows = block.rows();

    if (!num_rows)
        return;

    /// Build the data block: (id, timestamp, value).
    Block data_block;
    if (block.has(TimeSeriesColumnNames::Timestamp))
        data_block.insert(block.getByName(TimeSeriesColumnNames::Timestamp));
    if (block.has(TimeSeriesColumnNames::Value))
        data_block.insert(block.getByName(TimeSeriesColumnNames::Value));

    /// Build the tags block: (metric_name, tags, [all_tags], [tag columns], [min_time], [max_time]).
    Block tags_block;
    if (block.has(TimeSeriesColumnNames::MetricName))
        tags_block.insert(block.getByName(TimeSeriesColumnNames::MetricName));
    if (block.has(TimeSeriesColumnNames::Tags))
        tags_block.insert(block.getByName(TimeSeriesColumnNames::Tags));

    /// Add columns corresponding to specific tags specified in the "tags_to_columns" setting.
    const Map & tags_to_columns = (*time_series_settings)[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        if (block.has(column_name))
            tags_block.insert(block.getByName(column_name));
    }

    /// Add the all_tags column if needed for ID calculation.
    /// The ID default expression references all_tags (Map(String, String)),
    /// so it must be present in the tags_block with the correct type.
    if ((*time_series_settings)[TimeSeriesSetting::use_all_tags_column_to_generate_id])
    {
        /// The all_tags column is EPHEMERAL, so the input block will contain it but filled with
        /// empty maps (the default for an ephemeral column). We must always construct it from the
        /// actual tags data to compute correct IDs.
        if (block.has(TimeSeriesColumnNames::Tags))
        {
            /// Construct all_tags Map(String, String) from tags + individual tag columns.
            /// The tags column may be Map(LowCardinality(String), String), so we always build
            /// all_tags row by row using String keys to ensure correct types.
            const auto & tags_col = block.getByName(TimeSeriesColumnNames::Tags);
            auto all_tags_type = columns_description.get(TimeSeriesColumnNames::AllTags).type;

            const auto * tags_map = typeid_cast<const ColumnMap *>(tags_col.column.get());
            if (tags_map)
            {
                /// Create all_tags column using the type's own createColumn() to ensure correct structure.
                auto all_tags_mutable = all_tags_type->createColumn();
                auto & all_tags_column = typeid_cast<ColumnMap &>(*all_tags_mutable);
                IColumn & keys = all_tags_column.getNestedData().getColumn(0);
                IColumn & vals = all_tags_column.getNestedData().getColumn(1);
                auto & offsets = all_tags_column.getNestedColumn().getOffsets();

                const auto & src_nested = tags_map->getNestedData();
                const auto & src_keys = src_nested.getColumn(0);
                const auto & src_values = src_nested.getColumn(1);
                const auto & src_offsets = tags_map->getNestedColumn().getOffsets();

                for (size_t row = 0; row < num_rows; ++row)
                {
                    /// Copy existing tags (extracting from LowCardinality if needed).
                    size_t src_start = (row == 0) ? 0 : src_offsets[row - 1];
                    size_t src_end = src_offsets[row];
                    for (size_t j = src_start; j < src_end; ++j)
                    {
                        auto key = src_keys.getDataAt(j);
                        auto val = src_values.getDataAt(j);
                        keys.insertData(key.data(), key.size());
                        vals.insertData(val.data(), val.size());
                    }

                    /// Merge in tag columns from tags_to_columns setting.
                    for (const auto & entry : tags_to_columns)
                    {
                        const auto & tuple = entry.safeGet<Tuple>();
                        const auto & tag_name = tuple.at(0).safeGet<String>();
                        const auto & column_name = tuple.at(1).safeGet<String>();
                        if (block.has(column_name))
                        {
                            const auto & col = block.getByName(column_name).column;
                            auto val = col->getDataAt(row);
                            keys.insertData(tag_name.data(), tag_name.size());
                            vals.insertData(val.data(), val.size());
                        }
                    }

                    offsets.push_back(keys.size());
                }

                tags_block.insert(ColumnWithTypeAndName{
                    std::move(all_tags_mutable), all_tags_type, TimeSeriesColumnNames::AllTags});
            }
        }
    }

    /// Add min_time and max_time columns if present.
    if ((*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time])
    {
        if (block.has(TimeSeriesColumnNames::MinTime))
            tags_block.insert(block.getByName(TimeSeriesColumnNames::MinTime));
        if (block.has(TimeSeriesColumnNames::MaxTime))
            tags_block.insert(block.getByName(TimeSeriesColumnNames::MaxTime));
    }

    /// Calculate ID from metric_name and all_tags using the default expression.
    /// We always recalculate the ID because the input block may contain an ID computed from
    /// ephemeral columns (like all_tags) that were filled with defaults (empty maps) by the
    /// INSERT parser, making them incorrect. The correct ID must be based on the all_tags we
    /// just constructed from the actual tags data.
    {
        const auto & id_description = columns_description.get(TimeSeriesColumnNames::ID);
        IColumn & id_column_ref = calculateId(getContext(), id_description, tags_block);

        /// Also add the calculated ID to the data block.
        data_block.insert(ColumnWithTypeAndName{
            id_column_ref.getPtr(),
            id_description.type,
            TimeSeriesColumnNames::ID});
    }

    /// The "all_tags" column in the "tags" table is ephemeral - remove it before inserting.
    if (tags_block.has(TimeSeriesColumnNames::AllTags))
        tags_block.erase(TimeSeriesColumnNames::AllTags);

    /// Accumulate blocks - tags first, then data.
    /// (Because any INSERT can fail and we don't want to have rows in the data table
    /// with no corresponding "id" written to the "tags" table.)
    bool found_tags = false;
    bool found_data = false;
    for (auto & [kind, accumulated_block] : accumulated_blocks)
    {
        if (kind == ViewTarget::Tags && !found_tags)
        {
            MutableColumns cols = accumulated_block.mutateColumns();
            for (size_t i = 0; i < cols.size(); ++i)
            {
                const auto & col_name = accumulated_block.getByPosition(i).name;
                if (tags_block.has(col_name))
                {
                    const auto & src_col = tags_block.getByName(col_name);
                    cols[i]->insertRangeFrom(*src_col.column, 0, src_col.column->size());
                }
            }
            accumulated_block.setColumns(std::move(cols));
            found_tags = true;
        }
        else if (kind == ViewTarget::Data && !found_data)
        {
            MutableColumns cols = accumulated_block.mutateColumns();
            for (size_t i = 0; i < cols.size(); ++i)
            {
                const auto & col_name = accumulated_block.getByPosition(i).name;
                if (data_block.has(col_name))
                {
                    const auto & src_col = data_block.getByName(col_name);
                    cols[i]->insertRangeFrom(*src_col.column, 0, src_col.column->size());
                }
            }
            accumulated_block.setColumns(std::move(cols));
            found_data = true;
        }
    }

    if (!found_tags)
        accumulated_blocks.emplace_back(ViewTarget::Tags, std::move(tags_block));
    if (!found_data)
        accumulated_blocks.emplace_back(ViewTarget::Data, std::move(data_block));
}

void TimeSeriesSink::onFinish()
{
    insertToTargetTables();
}

void TimeSeriesSink::insertToTargetTables()
{
    auto time_series_storage_id = storage.getStorageID();

    for (auto & [table_kind, block] : accumulated_blocks)
    {
        if (!block.empty() && block.rows() > 0)
        {
            const auto & target_table_id = storage.getTargetTableId(table_kind);

            LOG_INFO(log, "{}: Inserting {} rows to the {} table",
                     time_series_storage_id.getNameForLogs(), block.rows(), toString(table_kind));

            auto insert_query = make_intrusive<ASTInsertQuery>();
            insert_query->table_id = target_table_id;

            auto columns_ast = make_intrusive<ASTExpressionList>();
            for (const auto & name : block.getNames())
                columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
            insert_query->columns = columns_ast;

            ContextMutablePtr insert_context = Context::createCopy(getContext());
            insert_context->setCurrentQueryId(getContext()->getCurrentQueryId() + ":" + String{toString(table_kind)});

            LOG_TEST(log, "{}: Executing query: {}", time_series_storage_id.getNameForLogs(), insert_query->formatForLogging());

            InterpreterInsertQuery interpreter(
                insert_query,
                insert_context,
                /* allow_materialized= */ false,
                /* no_squash= */ false,
                /* no_destination= */ false,
                /* async_insert= */ false);

            BlockIO io = interpreter.execute();
            PushingPipelineExecutor executor(io.pipeline);

            executor.start();
            executor.push(std::move(block));
            executor.finish();
        }
    }

    accumulated_blocks.clear();
}

}
