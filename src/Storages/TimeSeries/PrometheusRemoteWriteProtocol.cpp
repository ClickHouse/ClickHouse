#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <algorithm>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/addMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sources/BlocksSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsDataType timestamp_type;
    extern const TimeSeriesSettingsDataType scalar_type;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
}


namespace
{
    /// Sorts labels by name, removes exact duplicates and labels with empty values.
    /// Throws if any label name is empty, if two labels have the same name but different values,
    /// or if there is no label with name "__name__".
    /// Stores the result as string_view pairs pointing into the original protobuf data.
    void sortTagsAndRemoveDuplicates(
        const google::protobuf::RepeatedPtrField<prometheus::Label> & labels,
        std::vector<std::pair<std::string_view, std::string_view>> & res)
    {
        res.clear();
        res.reserve(labels.size());

        for (const auto & label : labels)
        {
            if (label.name().empty())
                throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Label name should not be empty");
            res.emplace_back(label.name(), label.value());
        }

        std::sort(res.begin(), res.end(), [](const auto & left, const auto & right) { return left.first < right.first; });
        res.erase(std::unique(res.begin(), res.end()), res.end());

        auto adjacent = std::adjacent_find(res.begin(), res.end(), [](const auto & left, const auto & right) { return left.first == right.first; });
        if (adjacent != res.end())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
                "Found two labels with the same name {} but different values {} and {}",
                adjacent->first, adjacent->second, std::next(adjacent)->second);
        }

        std::erase_if(res, [](const auto & x) { return x.second.empty(); });

        bool metric_name_found = std::any_of(res.begin(), res.end(),
            [](const auto & x) { return x.first == TimeSeriesTagNames::MetricName; });
        if (!metric_name_found)
            throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Metric name (label {}) not found", TimeSeriesTagNames::MetricName);
    }

    /// Calculates the identifier of each time series in "tags_block" using the default expression for the "id" column,
    /// and returns column "id" with the results.
    ColumnPtr calculateId(const ContextPtr & context, const TimeSeriesSettings & time_series_settings, const Block & tags_block)
    {
        DataTypePtr id_type = time_series_settings[TimeSeriesSetting::id_type];
        ColumnDescription id_column_description{TimeSeriesColumnNames::ID, id_type};
        id_column_description.default_desc.kind = ColumnDefaultKind::Default;
        id_column_description.default_desc.expression = time_series_settings[TimeSeriesSetting::id_generator].value;

        auto blocks = std::make_shared<Blocks>();
        blocks->push_back(tags_block);

        auto header = std::make_shared<const Block>(tags_block.cloneEmpty());
        auto pipe = Pipe(std::make_shared<BlocksSource>(blocks, header));

        Block header_with_id;
        const auto & id_name = id_column_description.name;
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

        return std::move(id_column);
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
            min_time = std::min(timestamp, min_time);
            max_time = std::max(timestamp, max_time);
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
                            const TimeSeriesSettings & time_series_settings,
                            const StorageInMemoryMetadata & tags_metadata,
                            const StorageInMemoryMetadata & samples_metadata)
    {
        size_t num_time_series = time_series.size();

        size_t num_samples = 0;
        for (const auto & element : time_series)
            num_samples += element.samples_size();

        if (!num_samples)
            return {}; /// Nothing to insert into target tables.

        /// Prepare a block for inserting to the "tags" table.
        DataTypePtr timestamp_type = samples_metadata.columns.get(TimeSeriesColumnNames::Timestamp).type;
        UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);

        /// Column "metric_name".
        DataTypePtr metric_name_type = tags_metadata.columns.get(TimeSeriesColumnNames::MetricName).type;
        auto metric_name_column = metric_name_type->createColumn();
        metric_name_column->reserve(num_time_series);

        /// Columns we should check explicitly that they're filled after filling each row.
        std::vector<IColumn *> columns_to_fill_in_tags_table;

        /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
        /// Keys are string_view into the settings data which lives for the duration of this function.
        std::unordered_map<std::string_view, std::pair<MutableColumnPtr, DataTypePtr>> columns_by_tag_name;
        const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            DataTypePtr column_type = tags_metadata.columns.get(column_name).type;
            auto column = column_type->createColumn();
            column->reserve(num_time_series);
            columns_to_fill_in_tags_table.emplace_back(column.get());
            columns_by_tag_name[tag_name] = {std::move(column), column_type};
        }

        /// Column "tags".
        auto tags_map_type = typeid_cast<std::shared_ptr<const DataTypeMap>>(tags_metadata.columns.get(TimeSeriesColumnNames::Tags).type);
        if (!tags_map_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have a Map type", TimeSeriesColumnNames::Tags);
        auto tags_names = tags_map_type->getKeyType()->createColumn();
        tags_names->reserve(num_time_series);
        auto tags_values = tags_map_type->getValueType()->createColumn();
        tags_values->reserve(num_time_series);
        auto tags_offsets = ColumnVector<IColumn::Offset>::create();
        tags_offsets->reserve(num_time_series);

        /// Column "all_tags".
        MutableColumnPtr all_tags_names;
        MutableColumnPtr all_tags_values;
        ColumnVector<IColumn::Offset>::MutablePtr all_tags_offsets;
        std::shared_ptr<const DataTypeMap> all_tags_map_type;
        if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
        {
            /// The "all_tags" column may not exist in external target tables.
            if (tags_metadata.columns.has(TimeSeriesColumnNames::AllTags))
                all_tags_map_type = typeid_cast<std::shared_ptr<const DataTypeMap>>(tags_metadata.columns.get(TimeSeriesColumnNames::AllTags).type);
            else
                all_tags_map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
            if (!all_tags_map_type)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have a Map type", TimeSeriesColumnNames::AllTags);
            all_tags_names = all_tags_map_type->getKeyType()->createColumn();
            all_tags_names->reserve(num_time_series);
            all_tags_values = all_tags_map_type->getValueType()->createColumn();
            all_tags_values->reserve(num_time_series);
            all_tags_offsets = ColumnVector<IColumn::Offset>::create();
            all_tags_offsets->reserve(num_time_series);
        }

        /// Columns "min_time" and "max_time".
        MutableColumnPtr min_time_column;
        MutableColumnPtr max_time_column;
        DataTypePtr min_time_type;
        DataTypePtr max_time_type;
        UInt32 min_time_scale = 0;
        UInt32 max_time_scale = 0;
        if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
        {
            min_time_type = tags_metadata.columns.get(TimeSeriesColumnNames::MinTime).type;
            max_time_type = tags_metadata.columns.get(TimeSeriesColumnNames::MaxTime).type;
            min_time_scale = tryGetDecimalScale(*removeNullable(min_time_type)).value_or(0);
            max_time_scale = tryGetDecimalScale(*removeNullable(max_time_type)).value_or(0);
            min_time_column = min_time_type->createColumn();
            max_time_column = max_time_type->createColumn();
            min_time_column->reserve(num_time_series);
            max_time_column->reserve(num_time_series);
            columns_to_fill_in_tags_table.emplace_back(min_time_column.get());
            columns_to_fill_in_tags_table.emplace_back(max_time_column.get());
        }

        std::vector<std::pair<std::string_view, std::string_view>> sorted_tags;

        /// Fill tag columns.
        size_t current_row_in_tags = 0;
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                continue;

            sortTagsAndRemoveDuplicates(element.labels(), sorted_tags);

            for (const auto [tag_name, tag_value] : sorted_tags)
            {
                if (tag_name == TimeSeriesTagNames::MetricName)
                {
                    metric_name_column->insertData(tag_value.data(), tag_value.size());
                }
                else
                {
                    if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                    {
                        all_tags_names->insertData(tag_name.data(), tag_name.size());
                        all_tags_values->insertData(tag_value.data(), tag_value.size());
                    }

                    auto it = columns_by_tag_name.find(tag_name);
                    bool has_column_for_tag_value = (it != columns_by_tag_name.end());
                    if (has_column_for_tag_value)
                    {
                        auto & column = it->second.first;
                        column->insertData(tag_value.data(), tag_value.size());
                    }
                    else
                    {
                        tags_names->insertData(tag_name.data(), tag_name.size());
                        tags_values->insertData(tag_value.data(), tag_value.size());
                    }
                }
            }

            tags_offsets->insertValue(tags_names->size());

            if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                all_tags_offsets->insertValue(all_tags_names->size());

            if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
            {
                auto [min_time, max_time] = findMinTimeAndMaxTime(element.samples());
                min_time_column->insert(DecimalUtils::convertTo<DateTime64>(min_time_scale, DateTime64{min_time}, 3));
                max_time_column->insert(DecimalUtils::convertTo<DateTime64>(max_time_scale, DateTime64{max_time}, 3));
            }

            for (auto * column : columns_to_fill_in_tags_table)
            {
                if (column->size() == current_row_in_tags)
                    column->insertDefault();
            }

            ++current_row_in_tags;
        }

        /// Build tags block.
        Block tags_block;
        tags_block.insert(ColumnWithTypeAndName{std::move(metric_name_column), metric_name_type, TimeSeriesColumnNames::MetricName});
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            auto & [column, column_type] = columns_by_tag_name.at(tag_name);
            tags_block.insert(ColumnWithTypeAndName{std::move(column), column_type, column_name});
        }
        Columns tags_tuple_cols;
        tags_tuple_cols.push_back(std::move(tags_names));
        tags_tuple_cols.push_back(std::move(tags_values));
        auto tags_column = ColumnMap::create(ColumnArray::create(ColumnTuple::create(std::move(tags_tuple_cols)), std::move(tags_offsets)));
        tags_block.insert(ColumnWithTypeAndName{std::move(tags_column), tags_map_type, TimeSeriesColumnNames::Tags});
        if (all_tags_names)
        {
            Columns all_tags_tuple_cols;
            all_tags_tuple_cols.push_back(std::move(all_tags_names));
            all_tags_tuple_cols.push_back(std::move(all_tags_values));
            auto all_tags_column = ColumnMap::create(ColumnArray::create(
                ColumnTuple::create(std::move(all_tags_tuple_cols)),
                std::move(all_tags_offsets)));
            tags_block.insert(ColumnWithTypeAndName{std::move(all_tags_column), all_tags_map_type, TimeSeriesColumnNames::AllTags});
        }
        if (min_time_column)
        {
            tags_block.insert(ColumnWithTypeAndName{std::move(min_time_column), min_time_type, TimeSeriesColumnNames::MinTime});
            tags_block.insert(ColumnWithTypeAndName{std::move(max_time_column), max_time_type, TimeSeriesColumnNames::MaxTime});
        }

        /// Calculate an identifier for each time series and add the result column to "tags_block".
        DataTypePtr id_type = tags_metadata.columns.get(TimeSeriesColumnNames::ID).type;
        auto id_column_in_tags_table = calculateId(context, time_series_settings, tags_block);
        tags_block.insert(0, ColumnWithTypeAndName{id_column_in_tags_table, id_type, TimeSeriesColumnNames::ID});

        /// The "all_tags" column in the "tags" table is either ephemeral or doesn't exists.
        /// We've used the "all_tags" column to calculate the "id" column already,
        /// and now we don't need it to insert to the "tags" table.
        tags_block.erase(TimeSeriesColumnNames::AllTags);

        /// Column "id".
        DataTypePtr samples_id_type = samples_metadata.columns.get(TimeSeriesColumnNames::ID).type;
        auto id_column_in_data_table = samples_id_type->createColumn();
        id_column_in_data_table->reserve(num_samples);

        /// Column "timestamp".
        auto timestamp_column = timestamp_type->createColumn();
        timestamp_column->reserve(num_samples);

        /// Column "value".
        DataTypePtr scalar_type = samples_metadata.columns.get(TimeSeriesColumnNames::Value).type;
        auto value_column = scalar_type->createColumn();
        value_column->reserve(num_samples);

        /// Prepare a block for inserting to the "samples" table.
        current_row_in_tags = 0;
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                continue;

            id_column_in_data_table->insertManyFrom(*id_column_in_tags_table, current_row_in_tags, element.samples_size());
            for (const auto & sample : element.samples())
            {
                timestamp_column->insert(DecimalUtils::convertTo<DateTime64>(timestamp_scale, DateTime64{sample.timestamp()}, 3));
                value_column->insert(sample.value());
            }

            ++current_row_in_tags;
        }

        /// Build data block.
        Block samples_block;
        samples_block.insert(ColumnWithTypeAndName{std::move(id_column_in_data_table), samples_id_type, TimeSeriesColumnNames::ID});
        samples_block.insert(ColumnWithTypeAndName{std::move(timestamp_column), timestamp_type, TimeSeriesColumnNames::Timestamp});
        samples_block.insert(ColumnWithTypeAndName{std::move(value_column), scalar_type, TimeSeriesColumnNames::Value});

        BlocksToInsert res;

        /// A block to the "tags" table should be inserted first.
        /// (Because any INSERT can fail and we don't want to have rows in the samples table with no corresponding "id" written to the "tags" table.)
        res.blocks.emplace_back(ViewTarget::Tags, std::move(tags_block));
        res.blocks.emplace_back(ViewTarget::Samples, std::move(samples_block));

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
                            const StorageInMemoryMetadata & metrics_table_metadata)
    {
        size_t num_rows = metrics_metadata.size();

        if (!num_rows)
            return {}; /// Nothing to insert into target tables.

        DataTypePtr metric_family_name_type = metrics_table_metadata.columns.get(TimeSeriesColumnNames::MetricFamilyName).type;
        DataTypePtr type_type = metrics_table_metadata.columns.get(TimeSeriesColumnNames::Type).type;
        DataTypePtr unit_type = metrics_table_metadata.columns.get(TimeSeriesColumnNames::Unit).type;
        DataTypePtr help_type = metrics_table_metadata.columns.get(TimeSeriesColumnNames::Help).type;

        auto metric_family_name_column = metric_family_name_type->createColumn();
        auto type_column = type_type->createColumn();
        auto unit_column = unit_type->createColumn();
        auto help_column = help_type->createColumn();

        metric_family_name_column->reserve(num_rows);
        type_column->reserve(num_rows);
        unit_column->reserve(num_rows);
        help_column->reserve(num_rows);

        for (const auto & element : metrics_metadata)
        {
            const auto & metric_family_name = element.metric_family_name();
            const auto type_str = metricTypeToString(element.type());
            const auto & unit = element.unit();
            const auto & help = element.help();

            metric_family_name_column->insertData(metric_family_name.data(), metric_family_name.size());
            type_column->insertData(type_str.data(), type_str.size());
            unit_column->insertData(unit.data(), unit.size());
            help_column->insertData(help.data(), help.size());
        }

        /// Prepare a result.
        Block block;
        block.insert(ColumnWithTypeAndName{std::move(metric_family_name_column), metric_family_name_type, TimeSeriesColumnNames::MetricFamilyName});
        block.insert(ColumnWithTypeAndName{std::move(type_column), type_type, TimeSeriesColumnNames::Type});
        block.insert(ColumnWithTypeAndName{std::move(unit_column), unit_type, TimeSeriesColumnNames::Unit});
        block.insert(ColumnWithTypeAndName{std::move(help_column), help_type, TimeSeriesColumnNames::Help});

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
            if (!block.empty())
            {
                const auto & target_table_id = time_series_storage.getTargetTableID(table_kind, context);

                LOG_INFO(log, "{}: Inserting {} rows to the {} table",
                         time_series_storage_id.getNameForLogs(), block.rows(), toString(table_kind));

                auto insert_query = make_intrusive<ASTInsertQuery>();
                insert_query->table_id = target_table_id;

                auto columns_ast = make_intrusive<ASTExpressionList>();
                for (const auto & name : block.getNames())
                    columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
                insert_query->columns = columns_ast;

                ContextMutablePtr insert_context = Context::createCopy(context);
                insert_context->setCurrentQueryId(fmt::format("{}:{}", context->getCurrentQueryId(), table_kind));

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

                // Convert block columns to match what the pipeline expect.
                const Block & expected_header = executor.getHeader();
                auto converting_dag = ActionsDAG::makeConvertingActions(
                    block.getColumnsWithTypeAndName(),
                    expected_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Name,
                    insert_context);
                auto converting_actions = std::make_shared<ExpressionActions>(
                    std::move(converting_dag), ExpressionActionsSettings(insert_context));
                converting_actions->execute(block);

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

    auto time_series_settings = time_series_storage->getStorageSettings();

    const auto & tags_metadata = *time_series_storage->getTargetTable(ViewTarget::Tags, getContext())->getInMemoryMetadataPtr(getContext(), false);
    const auto & samples_metadata = *time_series_storage->getTargetTable(ViewTarget::Samples, getContext())->getInMemoryMetadataPtr(getContext(), false);
    auto blocks = toBlocks(time_series, getContext(), *time_series_settings, tags_metadata, samples_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, getContext(), log.get());

    LOG_TRACE(log, "{}: {} time series written",
              time_series_storage_id.getNameForLogs(), time_series.size());
}

void PrometheusRemoteWriteProtocol::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    auto time_series_storage_id = time_series_storage->getStorageID();

    LOG_TRACE(log, "{}: Writing {} metrics metadata",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());

    const auto & metrics_table_metadata = *time_series_storage->getTargetTable(ViewTarget::Metrics, getContext())->getInMemoryMetadataPtr(getContext(), false);
    auto blocks = toBlocks(metrics_metadata, metrics_table_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, getContext(), log.get());

    LOG_TRACE(log, "{}: {} metrics metadata written",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());
}

}

#endif
