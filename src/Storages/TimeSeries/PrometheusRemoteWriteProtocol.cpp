#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
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
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <Storages/TimeSeries/TimeSeriesSink.h>
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
#include <base/EnumReflection.h>

#include <algorithm>
#include <limits>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


namespace
{
    /// Calculates the identifier of each time series in "tags_block"
    /// using the `id_generator` expression, and returns column `id` with the results.
    ColumnPtr calculateId(
        const Block & tags_block, const ContextPtr & context,
        const DataTypePtr & id_type, const ASTPtr & id_generator)
    {
        ColumnDescription id_column_description{TimeSeriesColumnNames::ID, id_type};
        id_column_description.default_desc.kind = ColumnDefaultKind::Default;
        id_column_description.default_desc.expression = id_generator;

        auto blocks = std::make_shared<Blocks>();
        blocks->push_back(tags_block);

        auto header = std::make_shared<const Block>(tags_block.cloneEmpty());
        auto pipe = Pipe(std::make_shared<BlocksSource>(blocks, header));

        Block id_header;
        const auto & id_name = id_column_description.name;
        id_header.insert(ColumnWithTypeAndName{id_type, id_name});

        auto calculate_id_dag = addMissingDefaults(
                    pipe.getHeader(),
                    id_header.getNamesAndTypesList(),
                    ColumnsDescription{id_column_description},
                    context);

        auto calculate_id_actions = std::make_shared<ExpressionActions>(std::move(calculate_id_dag));
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, calculate_id_actions);
        });

        auto convert_id_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            id_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            context);
        auto convert_id_actions = std::make_shared<ExpressionActions>(
            std::move(convert_id_dag),
            ExpressionActionsSettings(context, CompileExpressions::yes));
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, convert_id_actions);
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

    /// Finds the minimum timestamp in a time series.
    Int64 findMinTime(const google::protobuf::RepeatedPtrField<prometheus::Sample> & samples)
    {
        chassert(!samples.empty());
        Int64 min_time = std::numeric_limits<Int64>::max();
        for (const auto & sample : samples)
            min_time = std::min(sample.timestamp(), min_time);
        return min_time;
    }

    /// Finds the maximum timestamp in a time series.
    Int64 findMaxTime(const google::protobuf::RepeatedPtrField<prometheus::Sample> & samples)
    {
        chassert(!samples.empty());
        Int64 max_time = std::numeric_limits<Int64>::min();
        for (const auto & sample : samples)
            max_time = std::max(sample.timestamp(), max_time);
        return max_time;
    }

    /// Converts a protobuf metric type enum to its string representation.
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

    /// Returns true if a column contains DateTime64 or Nullable(DateTime64).
    bool isDateTime64Column(const IColumn & column)
    {
        const auto * ptr = &column;
        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(ptr))
            ptr = &nullable_column->getNestedColumn();
        return typeid_cast<const ColumnDecimal<DateTime64> *>(ptr) != nullptr;
    }

    /// Fills tag columns for the "tags" table by iterating over the time series.
    /// Optional output columns (out_all_tags_*) are skipped when null.
    void fillTagsColumns(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        IColumn & out_tags_names,
        IColumn & out_tags_values,
        ColumnVector<IColumn::Offset> & out_tags_offsets,
        std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name,
        IColumn * all_tags_names,
        IColumn * all_tags_values,
        ColumnVector<IColumn::Offset> * all_tags_offsets)
    {
        std::vector<std::pair<std::string_view, std::string_view>> sorted_tags;

        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];

            sorted_tags.clear();
            sorted_tags.reserve(element.labels().size());
            for (const auto & label : element.labels())
                sorted_tags.emplace_back(label.name(), label.value());

            TimeSeriesSink::sortTagsAndRemoveDuplicates(sorted_tags);

            TimeSeriesSink::insertSortedTagsToColumns(
                sorted_tags,
                out_tags_names, out_tags_values, out_tags_offsets,
                columns_by_tag_name,
                all_tags_names, all_tags_values, all_tags_offsets);
        }
    }

    /// Fills the min_time column for the "tags" table by iterating over the time series.
    /// T is the timestamp type: either DateTime64 (sub-second precision) or UInt32 (second precision).
    template <typename T>
    void fillMinTimeColumnImpl(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        UInt32 scale, IColumn & out_column)
    {
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                out_column.insertDefault();
            else if constexpr (is_decimal<T>)
                out_column.insert(DecimalUtils::convertTo<T>(scale, DateTime64{findMinTime(element.samples())}, 3));
            else
                out_column.insert(DecimalUtils::convertTo<T>(DateTime64{findMinTime(element.samples())}, 3));
        }
    }

    /// Fills the min_time column for the "tags" table by iterating over the time series.
    void fillMinTimeColumn(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        UInt32 scale, IColumn & out_column)
    {
        if (isDateTime64Column(out_column))
            fillMinTimeColumnImpl<DateTime64>(time_series, scale, out_column);
        else
            fillMinTimeColumnImpl<UInt32>(time_series, scale, out_column);
    }

    /// Fills the max_time column for the "tags" table by iterating over the time series.
    /// T is the timestamp type: either DateTime64 (sub-second precision) or UInt32 (second precision).
    template <typename T>
    void fillMaxTimeColumnImpl(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        UInt32 scale, IColumn & out_column)
    {
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                out_column.insertDefault();
            else if constexpr (is_decimal<T>)
                out_column.insert(DecimalUtils::convertTo<T>(scale, DateTime64{findMaxTime(element.samples())}, 3));
            else
                out_column.insert(DecimalUtils::convertTo<T>(DateTime64{findMaxTime(element.samples())}, 3));
        }
    }

    /// Fills the max_time column for the "tags" table by iterating over the time series.
    void fillMaxTimeColumn(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        UInt32 scale, IColumn & out_column)
    {
        if (isDateTime64Column(out_column))
            fillMaxTimeColumnImpl<DateTime64>(time_series, scale, out_column);
        else
            fillMaxTimeColumnImpl<UInt32>(time_series, scale, out_column);
    }

    /// Fills the id, timestamp, and value columns for the "samples" table by iterating over the time series.
    /// T is the timestamp type: either DateTime64 (sub-second precision) or UInt32 (second precision).
    template <typename T>
    void fillSamplesColumnsImpl(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        const IColumn & id_column_in_tags_table,
        IColumn & out_id_column,
        UInt32 timestamp_scale, IColumn & out_timestamp_column,
        IColumn & out_value_column)
    {
        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];
            if (!element.samples_size())
                continue;

            out_id_column.insertManyFrom(id_column_in_tags_table, i, element.samples_size());
            for (const auto & sample : element.samples())
            {
                if constexpr (is_decimal<T>)
                    out_timestamp_column.insert(DecimalUtils::convertTo<T>(timestamp_scale, DateTime64{sample.timestamp()}, 3));
                else
                    out_timestamp_column.insert(DecimalUtils::convertTo<T>(DateTime64{sample.timestamp()}, 3));
                out_value_column.insert(sample.value());
            }
        }
    }

    /// Fills the id, timestamp, and value columns for the "samples" table by iterating over the time series.
    void fillSamplesColumns(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        const IColumn & id_column_in_tags_table,
        IColumn & out_id_column,
        UInt32 timestamp_scale, IColumn & out_timestamp_column,
        IColumn & out_value_column)
    {
        if (isDateTime64Column(out_timestamp_column))
            fillSamplesColumnsImpl<DateTime64>(time_series, id_column_in_tags_table, out_id_column, timestamp_scale, out_timestamp_column, out_value_column);
        else
            fillSamplesColumnsImpl<UInt32>(time_series, id_column_in_tags_table, out_id_column, timestamp_scale, out_timestamp_column, out_value_column);
    }

    /// Fills the metric_family_name, type, unit, and help columns for the "metrics" table.
    void fillMetricsColumns(
        const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata,
        IColumn & out_metric_family_column,
        IColumn & out_type_column,
        IColumn & out_unit_column,
        IColumn & out_help_column)
    {
        for (const auto & element : metrics_metadata)
        {
            const auto & metric_family_name = element.metric_family_name();
            const auto type_str = metricTypeToString(element.type());
            const auto & unit = element.unit();
            const auto & help = element.help();

            out_metric_family_column.insertData(metric_family_name.data(), metric_family_name.size());
            out_type_column.insertData(type_str.data(), type_str.size());
            out_unit_column.insertData(unit.data(), unit.size());
            out_help_column.insertData(help.data(), help.size());
        }
    }

    struct BlocksToInsert
    {
        std::vector<std::pair<ViewTarget::Kind, Block>> blocks;
    };

    /// Converts time series from the protobuf format to prepared blocks for inserting into target tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
                            const ContextPtr & context,
                            const StorageTimeSeries & time_series_storage,
                            const TimeSeriesSettings & time_series_settings,
                            const StorageInMemoryMetadata & tags_metadata,
                            const StorageInMemoryMetadata & samples_metadata)
    {
        size_t num_time_series = time_series.size();
        if (!num_time_series)
            return {}; /// Nothing to insert into target tables.

        /// Prepare a block for inserting to the "tags" table.
        DataTypePtr timestamp_type = samples_metadata.columns.get(TimeSeriesColumnNames::Timestamp).type;
        UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);

        /// Resolve the expression for generating `id`.
        const auto & tags_id_col = tags_metadata.columns.get(TimeSeriesColumnNames::ID);
        DataTypePtr id_type = tags_id_col.type;
        ASTPtr id_generator = time_series_settings[TimeSeriesSetting::id_generator].value;
        if (!id_generator)
            id_generator = tags_id_col.default_desc.expression;
        if (!id_generator)
            id_generator = TimeSeriesIDGenerator::getDefault(id_type, time_series_settings, time_series_storage.getStorageID());
        const bool id_generator_uses_all_tags = TimeSeriesIDGenerator::usesAllTags(id_generator);

        /// Column `metric_name` and also the columns from the `tags_to_columns` settings.
        /// Keys are string_view into the settings data which lives for the duration of this function.
        std::vector<std::tuple<String, MutableColumnPtr, DataTypePtr>> columns_by_tag_name_holder;
        std::unordered_map<std::string_view, IColumn *> columns_by_tag_name;

        auto add_tag_column = [&](std::string_view tag_name, const String & column_name)
        {
            DataTypePtr column_type = tags_metadata.columns.get(column_name).type;
            auto column = column_type->createColumn();
            column->reserve(num_time_series);
            columns_by_tag_name[tag_name] = column.get();
            columns_by_tag_name_holder.emplace_back(column_name, std::move(column), column_type);
        };

        add_tag_column(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);

        const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            add_tag_column(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
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
        if (id_generator_uses_all_tags)
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
        }

        /// Fill tag columns.
        fillTagsColumns(
            time_series,
            *tags_names, *tags_values, *tags_offsets,
            columns_by_tag_name,
            all_tags_names.get(), all_tags_values.get(), all_tags_offsets.get());

        /// Fill min_time and max_time columns.
        if (min_time_column)
            fillMinTimeColumn(time_series, min_time_scale, *min_time_column);

        if (max_time_column)
            fillMaxTimeColumn(time_series, max_time_scale, *max_time_column);

        /// Build tags block.
        Block tags_block;
        for (auto & [column_name, column, column_type] : columns_by_tag_name_holder)
            tags_block.insert(ColumnWithTypeAndName{std::move(column), column_type, column_name});
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
        auto id_column_in_tags_table = calculateId(tags_block, context, id_type, id_generator);
        tags_block.insert(0, ColumnWithTypeAndName{id_column_in_tags_table, id_type, TimeSeriesColumnNames::ID});

        /// The "all_tags" column in the "tags" table is either ephemeral or doesn't exist.
        /// We've used the "all_tags" column to calculate the "id" column already,
        /// and now we don't need it to insert to the "tags" table.
        if (tags_block.has(TimeSeriesColumnNames::AllTags))
            tags_block.erase(TimeSeriesColumnNames::AllTags);

        size_t total_samples = 0;
        for (const auto & element : time_series)
            total_samples += element.samples_size();

        /// Column "id".
        auto id_column_in_data_table = id_type->createColumn();
        id_column_in_data_table->reserve(total_samples);

        /// Column "timestamp".
        auto timestamp_column = timestamp_type->createColumn();
        timestamp_column->reserve(total_samples);

        /// Column "value".
        DataTypePtr scalar_type = samples_metadata.columns.get(TimeSeriesColumnNames::Value).type;
        auto value_column = scalar_type->createColumn();
        value_column->reserve(total_samples);

        /// Prepare a block for inserting to the "samples" table.
        fillSamplesColumns(time_series, *id_column_in_tags_table,
                           *id_column_in_data_table,
                           timestamp_scale, *timestamp_column,
                           *value_column);

        /// Build data block.
        Block samples_block;
        samples_block.insert(ColumnWithTypeAndName{std::move(id_column_in_data_table), id_type, TimeSeriesColumnNames::ID});
        samples_block.insert(ColumnWithTypeAndName{std::move(timestamp_column), timestamp_type, TimeSeriesColumnNames::Timestamp});
        samples_block.insert(ColumnWithTypeAndName{std::move(value_column), scalar_type, TimeSeriesColumnNames::Value});

        BlocksToInsert res;

        /// A block to the "tags" table should be inserted first.
        /// (Because any INSERT can fail and we don't want to have rows in the samples table with no corresponding "id" written to the "tags" table.)
        res.blocks.emplace_back(ViewTarget::Tags, std::move(tags_block));
        res.blocks.emplace_back(ViewTarget::Samples, std::move(samples_block));

        return res;
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

        fillMetricsColumns(metrics_metadata, *metric_family_name_column, *type_column, *unit_column, *help_column);

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
            if (block.rows() > 0)
            {
                const auto & target_table_id = time_series_storage.getTargetTableID(table_kind, context);

                LOG_INFO(log, "{}: Inserting {} rows to the {} table",
                         time_series_storage_id.getNameForLogs(), block.rows(), table_kind);

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

                // Convert block columns to match what the pipeline expects.
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

    auto tags_table_metadata = time_series_storage->getTargetTable(ViewTarget::Tags, getContext())->getInMemoryMetadataPtr(getContext(), false);
    auto samples_table_metadata = time_series_storage->getTargetTable(ViewTarget::Samples, getContext())->getInMemoryMetadataPtr(getContext(), false);
    const auto & tags_metadata = *tags_table_metadata;
    const auto & samples_metadata = *samples_table_metadata;
    auto blocks = toBlocks(time_series, getContext(), *time_series_storage, *time_series_settings, tags_metadata, samples_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, getContext(), log.get());

    LOG_TRACE(log, "{}: {} time series written",
              time_series_storage_id.getNameForLogs(), time_series.size());
}

void PrometheusRemoteWriteProtocol::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    auto time_series_storage_id = time_series_storage->getStorageID();

    LOG_TRACE(log, "{}: Writing {} metrics metadata",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());

    auto metrics_table_metadata_ptr = time_series_storage->getTargetTable(ViewTarget::Metrics, getContext())->getInMemoryMetadataPtr(getContext(), false);
    const auto & metrics_table_metadata = *metrics_table_metadata_ptr;
    auto blocks = toBlocks(metrics_metadata, metrics_table_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, getContext(), log.get());

    LOG_TRACE(log, "{}: {} metrics metadata written",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());
}

}

#endif
