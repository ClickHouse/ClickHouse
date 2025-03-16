#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesColumnsValidator.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/addMissingDefaults.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sources/BlocksSource.h>
#include <Processors/Transforms/ExpressionTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int ILLEGAL_COLUMN;
}


namespace
{
    /// Checks that a specified set of labels is sorted and has no duplications, and there is one label named "__name__".
    void checkLabels(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels)
    {
        bool metric_name_found = false;
        for (size_t i = 0; i != static_cast<size_t>(labels.size()); ++i)
        {
            const auto & label = labels[static_cast<int>(i)];
            const auto & label_name = label.name();
            const auto & label_value = label.value();

            if (label_name.empty())
                throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Label name should not be empty");
            if (label_value.empty())
                continue; /// Empty label value is treated like the label doesn't exist.

            if (label_name == TimeSeriesTagNames::MetricName)
                metric_name_found = true;

            if (i)
            {
                /// Check that labels are sorted.
                const auto & previous_label_name = labels[static_cast<int>(i - 1)].name();
                if (label_name <= previous_label_name)
                {
                    if (label_name == previous_label_name)
                        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Found duplicate label {}", label_name);
                    else
                        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Label names are not sorted in lexicographical order ({} > {})",
                                        previous_label_name, label_name);
                }
            }
        }

        if (!metric_name_found)
            throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Metric name (label {}) not found", TimeSeriesTagNames::MetricName);
    }

    /// Finds the description of an insertable column in the list.
    const ColumnDescription & getInsertableColumnDescription(const ColumnsDescription & columns, const String & column_name, const StorageID & time_series_storage_id)
    {
        const ColumnDescription * column = columns.tryGet(column_name);
        if (!column || ((column->default_desc.kind != ColumnDefaultKind::Default) && (column->default_desc.kind != ColumnDefaultKind::Ephemeral)))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: Column {} {}",
                            time_series_storage_id.getNameForLogs(), column_name, column ? "non-insertable" : "doesn't exist");
        }
        return *column;
    }

    /// Calculates the identifier of each time series in "tags_block" using the default expression for the "id" column,
    /// and adds column "id" with the results to "tags_block".
    IColumn & calculateId(const ContextPtr & context, const ColumnDescription & id_column_description, Block & tags_block)
    {
        auto blocks = std::make_shared<Blocks>();
        blocks->push_back(tags_block);

        auto header = tags_block.cloneEmpty();
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
        pipe.addSimpleTransform([&](const Block & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, adding_missing_defaults_actions);
        });

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            header_with_id.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto actions = std::make_shared<ExpressionActions>(
            std::move(convert_actions_dag),
            ExpressionActionsSettings::fromContext(context, CompileExpressions::yes));
        pipe.addSimpleTransform([&](const Block & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, actions);
        });

        QueryPipeline pipeline{std::move(pipe)};
        PullingPipelineExecutor executor{pipeline};

        MutableColumnPtr id_column;

        Block block_from_executor;
        while (executor.pull(block_from_executor))
        {
            if (block_from_executor)
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

    /// Converts a timestamp in milliseconds to a DateTime64 with a specified scale.
    DateTime64 scaleTimestamp(Int64 timestamp_ms, UInt32 scale)
    {
        if (scale == 3)
            return timestamp_ms;
        else if (scale > 3)
            return timestamp_ms * DecimalUtils::scaleMultiplier<DateTime64>(scale - 3);
        else
            return timestamp_ms / DecimalUtils::scaleMultiplier<DateTime64>(3 - scale);
    }

    /// Finds min time and max time in a time series.
    std::pair<Int64, Int64> findMinTimeAndMaxTime(const google::protobuf::RepeatedPtrField<prometheus::Sample> & samples)
    {
        chassert(!samples.empty());
        Int64 min_time = std::numeric_limits<Int64>::max();
        Int64 max_time = std::numeric_limits<Int64>::min();
        for (const auto & sample : samples)
        {
            Int64 timestamp = sample.timestamp();
            if (timestamp < min_time)
                min_time = timestamp;
            if (timestamp > max_time)
                max_time = timestamp;
        }
        return {min_time, max_time};
    }

    struct BlocksToInsert
    {
        std::vector<std::pair<ViewTarget::Kind, Block>> blocks;
    };

    /// Converts time series from the protobuf format to prepared blocks for inserting into target tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
                            const ContextPtr & context,
                            const StorageID & time_series_storage_id,
                            const StorageInMemoryMetadata & time_series_storage_metadata,
                            const TimeSeriesSettings & time_series_settings)
    {
        size_t num_tags_rows = time_series.size();

        size_t num_data_rows = 0;
        for (const auto & element : time_series)
            num_data_rows += element.samples_size();

        if (!num_data_rows)
            return {}; /// Nothing to insert into target tables.

        /// Column types must be extracted from the target tables' metadata.
        const auto & columns_description = time_series_storage_metadata.columns;

        auto get_column_description = [&](const String & column_name) -> const ColumnDescription &
        {
            return getInsertableColumnDescription(columns_description, column_name, time_series_storage_id);
        };

        /// We're going to prepare two blocks - one for the "data" table, and one for the "tags" table.
        Block data_block, tags_block;

        auto make_column_for_data_block = [&](const ColumnDescription & column_description) -> IColumn &
        {
            auto column = column_description.type->createColumn();
            column->reserve(num_data_rows);
            auto * column_ptr = column.get();
            data_block.insert(ColumnWithTypeAndName{std::move(column), column_description.type, column_description.name});
            return *column_ptr;
        };

        auto make_column_for_tags_block = [&](const ColumnDescription & column_description) -> IColumn &
        {
            auto column = column_description.type->createColumn();
            column->reserve(num_tags_rows);
            auto * column_ptr = column.get();
            tags_block.insert(ColumnWithTypeAndName{std::move(column), column_description.type, column_description.name});
            return *column_ptr;
        };

        /// Create columns.

        /// Column "id".
        const auto & id_description = get_column_description(TimeSeriesColumnNames::ID);
        TimeSeriesColumnsValidator validator{time_series_storage_id, time_series_settings};
        validator.validateColumnForID(id_description);
        auto & id_column_in_data_table = make_column_for_data_block(id_description);

        /// Column "timestamp".
        const auto & timestamp_description = get_column_description(TimeSeriesColumnNames::Timestamp);
        UInt32 timestamp_scale;
        validator.validateColumnForTimestamp(timestamp_description, timestamp_scale);
        auto & timestamp_column = make_column_for_data_block(timestamp_description);

        /// Column "value".
        const auto & value_description = get_column_description(TimeSeriesColumnNames::Value);
        validator.validateColumnForValue(value_description);
        auto & value_column = make_column_for_data_block(value_description);

        /// Column "metric_name".
        const auto & metric_name_description = get_column_description(TimeSeriesColumnNames::MetricName);
        validator.validateColumnForMetricName(metric_name_description);
        auto & metric_name_column = make_column_for_tags_block(metric_name_description);

        /// Columns we should check explicitly that they're filled after filling each row.
        std::vector<IColumn *> columns_to_fill_in_tags_table;

        /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
        std::unordered_map<String, IColumn *> columns_by_tag_name;
        const Map & tags_to_columns = time_series_settings.tags_to_columns;
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            const auto & column_description = get_column_description(column_name);
            validator.validateColumnForTagValue(column_description);
            auto & column = make_column_for_tags_block(column_description);
            columns_by_tag_name[tag_name] = &column;
            columns_to_fill_in_tags_table.emplace_back(&column);
        }

        /// Column "tags".
        const auto & tags_description = get_column_description(TimeSeriesColumnNames::Tags);
        validator.validateColumnForTagsMap(tags_description);
        auto & tags_column = typeid_cast<ColumnMap &>(make_column_for_tags_block(tags_description));
        IColumn & tags_names = tags_column.getNestedData().getColumn(0);
        IColumn & tags_values = tags_column.getNestedData().getColumn(1);
        auto & tags_offsets = tags_column.getNestedColumn().getOffsets();

        /// Column "all_tags".
        IColumn * all_tags_names = nullptr;
        IColumn * all_tags_values = nullptr;
        IColumn::Offsets * all_tags_offsets = nullptr;
        if (time_series_settings.use_all_tags_column_to_generate_id)
        {
            const auto & all_tags_description = get_column_description(TimeSeriesColumnNames::AllTags);
            validator.validateColumnForTagsMap(all_tags_description);
            auto & all_tags_column = typeid_cast<ColumnMap &>(make_column_for_tags_block(all_tags_description));
            all_tags_names = &all_tags_column.getNestedData().getColumn(0);
            all_tags_values = &all_tags_column.getNestedData().getColumn(1);
            all_tags_offsets = &all_tags_column.getNestedColumn().getOffsets();
        }

        /// Columns "min_time" and "max_time".
        IColumn * min_time_column = nullptr;
        IColumn * max_time_column = nullptr;
        UInt32 min_time_scale = 0;
        UInt32 max_time_scale = 0;
        if (time_series_settings.store_min_time_and_max_time)
        {
            const auto & min_time_description = get_column_description(TimeSeriesColumnNames::MinTime);
            const auto & max_time_description = get_column_description(TimeSeriesColumnNames::MaxTime);
            validator.validateColumnForTimestamp(min_time_description, min_time_scale);
            validator.validateColumnForTimestamp(max_time_description, max_time_scale);
            min_time_column = &make_column_for_tags_block(min_time_description);
            max_time_column = &make_column_for_tags_block(max_time_description);
            columns_to_fill_in_tags_table.emplace_back(min_time_column);
            columns_to_fill_in_tags_table.emplace_back(max_time_column);
        }

        /// Prepare a block for inserting into the "tags" table.
        size_t current_row_in_tags = 0;
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                continue;

            const auto & labels = element.labels();
            checkLabels(labels);

            for (size_t j = 0; j != static_cast<size_t>(labels.size()); ++j)
            {
                const auto & label = labels[static_cast<int>(j)];
                const auto & tag_name = label.name();
                const auto & tag_value = label.value();

                if (tag_name == TimeSeriesTagNames::MetricName)
                {
                    metric_name_column.insertData(tag_value.data(), tag_value.length());
                }
                else
                {
                    if (time_series_settings.use_all_tags_column_to_generate_id)
                    {
                        all_tags_names->insertData(tag_name.data(), tag_name.length());
                        all_tags_values->insertData(tag_value.data(), tag_value.length());
                    }

                    auto it = columns_by_tag_name.find(tag_name);
                    bool has_column_for_tag_value = (it != columns_by_tag_name.end());
                    if (has_column_for_tag_value)
                    {
                        auto * column = it->second;
                        column->insertData(tag_value.data(), tag_value.length());
                    }
                    else
                    {
                        tags_names.insertData(tag_name.data(), tag_name.length());
                        tags_values.insertData(tag_value.data(), tag_value.length());
                    }
                }
            }

            tags_offsets.push_back(tags_names.size());

            if (time_series_settings.use_all_tags_column_to_generate_id)
                all_tags_offsets->push_back(all_tags_names->size());

            if (time_series_settings.store_min_time_and_max_time)
            {
                auto [min_time, max_time] = findMinTimeAndMaxTime(element.samples());
                min_time_column->insert(scaleTimestamp(min_time, min_time_scale));
                max_time_column->insert(scaleTimestamp(max_time, max_time_scale));
            }

            for (auto * column : columns_to_fill_in_tags_table)
            {
                if (column->size() == current_row_in_tags)
                    column->insertDefault();
            }

            ++current_row_in_tags;
        }

        /// Calculate an identifier for each time series, make a new column from those identifiers, and add it to "tags_block".
        auto & id_column_in_tags_table = calculateId(context, columns_description.get(TimeSeriesColumnNames::ID), tags_block);

        /// Prepare a block for inserting to the "data" table.
        current_row_in_tags = 0;
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                continue;

            id_column_in_data_table.insertManyFrom(id_column_in_tags_table, current_row_in_tags, element.samples_size());
            for (const auto & sample : element.samples())
            {
                timestamp_column.insert(scaleTimestamp(sample.timestamp(), timestamp_scale));
                value_column.insert(sample.value());
            }

            ++current_row_in_tags;
        }

        /// The "all_tags" column in the "tags" table is either ephemeral or doesn't exists.
        /// We've used the "all_tags" column to calculate the "id" column already,
        /// and now we don't need it to insert to the "tags" table.
        tags_block.erase(TimeSeriesColumnNames::AllTags);

        BlocksToInsert res;

        /// A block to the "tags" table should be inserted first.
        /// (Because any INSERT can fail and we don't want to have rows in the data table with no corresponding "id" written to the "tags" table.)
        res.blocks.emplace_back(ViewTarget::Tags, std::move(tags_block));
        res.blocks.emplace_back(ViewTarget::Data, std::move(data_block));

        return res;
    }

    std::string_view metricTypeToString(prometheus::MetricMetadata::MetricType metric_type)
    {
        using namespace std::literals;
        switch (metric_type)
        {
            case prometheus::MetricMetadata::UNKNOWN: return "unknown"sv;
            case prometheus::MetricMetadata::COUNTER: return "counter"sv;
            case prometheus::MetricMetadata::GAUGE: return "gauge"sv;
            case prometheus::MetricMetadata::HISTOGRAM: return "histogram"sv;
            case prometheus::MetricMetadata::GAUGEHISTOGRAM: return "gaugehistogram"sv;
            case prometheus::MetricMetadata::SUMMARY: return "summary"sv;
            case prometheus::MetricMetadata::INFO: return "info"sv;
            case prometheus::MetricMetadata::STATESET: return "stateset"sv;
            default: break;
        }
        return "";
    }

    /// Converts metrics metadata from the protobuf format to prepared blocks for inserting into target tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata,
                            const StorageID & time_series_storage_id,
                            const StorageInMemoryMetadata & time_series_storage_metadata,
                            const TimeSeriesSettings & time_series_settings)
    {
        size_t num_rows = metrics_metadata.size();

        if (!num_rows)
            return {}; /// Nothing to insert into target tables.

        /// Column types must be extracted from the target tables' metadata.
        const auto & columns_description = time_series_storage_metadata.columns;

        auto get_column_description = [&](const String & column_name) -> const ColumnDescription &
        {
            return getInsertableColumnDescription(columns_description, column_name, time_series_storage_id);
        };

        /// We're going to prepare one blocks for the "metrics" table.
        Block block;

        auto make_column = [&](const ColumnDescription & column_description) -> IColumn &
        {
            auto column = column_description.type->createColumn();
            column->reserve(num_rows);
            auto * column_ptr = column.get();
            block.insert(ColumnWithTypeAndName{std::move(column), column_description.type, column_description.name});
            return *column_ptr;
        };

        /// Create columns.

        /// Column "metric_family_name".
        const auto & metric_family_name_description = get_column_description(TimeSeriesColumnNames::MetricFamilyName);
        TimeSeriesColumnsValidator validator{time_series_storage_id, time_series_settings};
        validator.validateColumnForMetricFamilyName(metric_family_name_description);
        auto & metric_family_name_column = make_column(metric_family_name_description);

        /// Column "type".
        const auto & type_description = get_column_description(TimeSeriesColumnNames::Type);
        validator.validateColumnForType(type_description);
        auto & type_column = make_column(type_description);

        /// Column "unit".
        const auto & unit_description = get_column_description(TimeSeriesColumnNames::Unit);
        validator.validateColumnForUnit(unit_description);
        auto & unit_column = make_column(unit_description);

        /// Column "help".
        const auto & help_description = get_column_description(TimeSeriesColumnNames::Help);
        validator.validateColumnForHelp(help_description);
        auto & help_column = make_column(help_description);

        /// Fill those columns.
        for (const auto & element : metrics_metadata)
        {
            const auto & metric_family_name = element.metric_family_name();
            const auto & type_str = metricTypeToString(element.type());
            const auto & help = element.help();
            const auto & unit = element.unit();

            metric_family_name_column.insertData(metric_family_name.data(), metric_family_name.length());
            type_column.insertData(type_str.data(), type_str.length());
            unit_column.insertData(unit.data(), unit.length());
            help_column.insertData(help.data(), help.length());
        }

        /// Prepare a result.
        BlocksToInsert res;
        res.blocks.emplace_back(ViewTarget::Metrics, std::move(block));
        return res;
    }

    /// Inserts blocks to target tables.
    void insertToTargetTables(BlocksToInsert && blocks, StorageTimeSeries & time_series_storage, ContextPtr context, Poco::Logger * log)
    {
        auto time_series_storage_id = time_series_storage.getStorageID();

        for (auto & [table_kind, block] : blocks.blocks)
        {
            if (block)
            {
                const auto & target_table_id = time_series_storage.getTargetTableId(table_kind);

                LOG_INFO(log, "{}: Inserting {} rows to the {} table",
                         time_series_storage_id.getNameForLogs(), block.rows(), toString(table_kind));

                auto insert_query = std::make_shared<ASTInsertQuery>();
                insert_query->table_id = target_table_id;

                auto columns_ast = std::make_shared<ASTExpressionList>();
                for (const auto & name : block.getNames())
                    columns_ast->children.emplace_back(std::make_shared<ASTIdentifier>(name));
                insert_query->columns = columns_ast;

                ContextMutablePtr insert_context = Context::createCopy(context);
                insert_context->setCurrentQueryId(context->getCurrentQueryId() + ":" + String{toString(table_kind)});

                LOG_TEST(log, "{}: Executing query: {}", time_series_storage_id.getNameForLogs(), queryToString(insert_query));

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
    }
}


PrometheusRemoteWriteProtocol::PrometheusRemoteWriteProtocol(StoragePtr time_series_storage_, const ContextPtr & context_)
    : WithContext(context_)
    , time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , log(getLogger("PrometheusRemoteWriteProtocol"))
{
}

PrometheusRemoteWriteProtocol::~PrometheusRemoteWriteProtocol() = default;


void PrometheusRemoteWriteProtocol::writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series)
{
    auto time_series_storage_id = time_series_storage->getStorageID();

    LOG_TRACE(log, "{}: Writing {} time series",
              time_series_storage_id.getNameForLogs(), time_series.size());

    auto time_series_storage_metadata = time_series_storage->getInMemoryMetadataPtr();
    auto time_series_settings = time_series_storage->getStorageSettingsPtr();

    auto blocks = toBlocks(time_series, getContext(), time_series_storage_id, *time_series_storage_metadata, *time_series_settings);
    insertToTargetTables(std::move(blocks), *time_series_storage, getContext(), log.get());

    LOG_TRACE(log, "{}: {} time series written",
              time_series_storage_id.getNameForLogs(), time_series.size());
}

void PrometheusRemoteWriteProtocol::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    auto time_series_storage_id = time_series_storage->getStorageID();

    LOG_TRACE(log, "{}: Writing {} metrics metadata",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());

    auto time_series_storage_metadata = time_series_storage->getInMemoryMetadataPtr();
    auto time_series_settings = time_series_storage->getStorageSettingsPtr();

    auto blocks = toBlocks(metrics_metadata, time_series_storage_id, *time_series_storage_metadata, *time_series_settings);
    insertToTargetTables(std::move(blocks), *time_series_storage, getContext(), log.get());

    LOG_TRACE(log, "{}: {} metrics metadata written",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());
}

}

#endif
