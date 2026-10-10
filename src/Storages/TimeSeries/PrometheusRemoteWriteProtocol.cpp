#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <Common/saturatedDuration.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/Progress.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <prompb/io/prometheus/write/v2/types.pb.h>

#include <chrono>
#include <vector>


namespace DB
{

namespace Setting
{
    extern const SettingsBool async_insert;
    extern const SettingsSeconds wait_for_async_insert_timeout;
}

namespace ErrorCodes
{
    extern const int ASYNC_INSERT_FLUSH_TIMEOUT;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int LOGICAL_ERROR;
}

namespace
{

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

std::string_view metricTypeToString(io::prometheus::write::v2::Metadata::MetricType metric_type)
{
    return metricTypeToString(static_cast<prometheus::MetricMetadata::MetricType>(metric_type));
}

void insertTimestamp(Int64 timestamp_ms, UInt32 scale, IColumn & column)
{
    if (typeid_cast<ColumnDecimal<DateTime64> *>(&column))
        column.insert(DecimalUtils::convertTo<DateTime64>(scale, DateTime64{timestamp_ms}, 3));
    else
        column.insert(DecimalUtils::convertTo<UInt32>(DateTime64{timestamp_ms}, 3));
}

class TimeSeriesBlockBuilder
{
public:
    TimeSeriesBlockBuilder(size_t num_rows, const StorageInMemoryMetadata & metadata, const String & samples_column_name)
        : metric_name_type(metadata.columns.get(TimeSeriesColumnNames::MetricName).type)
        , metric_name_column(metric_name_type->createColumn())
        , tags_type(typeid_cast<std::shared_ptr<const DataTypeMap>>(metadata.columns.get(TimeSeriesColumnNames::Tags).type))
        , time_series_type(typeid_cast<std::shared_ptr<const DataTypeArray>>(metadata.columns.get(samples_column_name).type))
    {
        if (!tags_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have a Map type", TimeSeriesColumnNames::Tags);
        if (!time_series_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have an Array type", samples_column_name);

        metric_name_column->reserve(num_rows);
        tags_names = tags_type->getKeyType()->createColumn();
        tags_values = tags_type->getValueType()->createColumn();
        tags_offsets->reserve(num_rows);

        auto [timestamp_type, value_type] = splitTimeSeriesType(time_series_type);
        timestamps = timestamp_type->createColumn();
        values = value_type->createColumn();
        time_series_offsets->reserve(num_rows);
        timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);
    }

    void addLabel(std::string_view name, std::string_view value)
    {
        if (metric_name.empty() && name == TimeSeriesTagNames::MetricName && !value.empty())
            metric_name = value;
        else
        {
            tags_names->insertData(name.data(), name.size());
            tags_values->insertData(value.data(), value.size());
        }
    }

    void addSample(Int64 timestamp, double value)
    {
        insertTimestamp(timestamp, timestamp_scale, *timestamps);
        values->insert(value);
    }

    void finishTimeSeries(int missing_metric_name_error_code)
    {
        if (metric_name.empty())
            throw Exception(
                missing_metric_name_error_code,
                "Metric name is missing: a time series has no `{}` label with a non-empty value",
                TimeSeriesTagNames::MetricName);
        metric_name_column->insertData(metric_name.data(), metric_name.size());
        tags_offsets->insert(tags_names->size());
        time_series_offsets->insert(timestamps->size());
        metric_name = {};
    }

    Block finish(size_t num_metadata_rows, const String & samples_column_name)
    {
        metric_name_column->insertManyDefaults(num_metadata_rows);
        for (size_t i = 0; i != num_metadata_rows; ++i)
        {
            tags_offsets->insert(tags_names->size());
            time_series_offsets->insert(timestamps->size());
        }

        Columns tags_tuple_columns;
        tags_tuple_columns.push_back(std::move(tags_names));
        tags_tuple_columns.push_back(std::move(tags_values));
        auto tags_column = ColumnMap::create(
            ColumnArray::create(ColumnTuple::create(std::move(tags_tuple_columns)), std::move(tags_offsets)));

        Columns time_series_tuple_columns;
        time_series_tuple_columns.push_back(std::move(timestamps));
        time_series_tuple_columns.push_back(std::move(values));
        auto time_series_column = ColumnArray::create(
            ColumnTuple::create(std::move(time_series_tuple_columns)), std::move(time_series_offsets));

        Block block;
        block.insert(ColumnWithTypeAndName{std::move(metric_name_column), metric_name_type, TimeSeriesColumnNames::MetricName});
        block.insert(ColumnWithTypeAndName{std::move(tags_column), tags_type, TimeSeriesColumnNames::Tags});
        block.insert(ColumnWithTypeAndName{std::move(time_series_column), time_series_type, samples_column_name});
        return block;
    }

private:
    DataTypePtr metric_name_type;
    MutableColumnPtr metric_name_column;
    std::shared_ptr<const DataTypeMap> tags_type;
    MutableColumnPtr tags_names;
    MutableColumnPtr tags_values;
    MutableColumnPtr tags_offsets = ColumnArray::ColumnOffsets::create();
    std::shared_ptr<const DataTypeArray> time_series_type;
    MutableColumnPtr timestamps;
    MutableColumnPtr values;
    MutableColumnPtr time_series_offsets = ColumnArray::ColumnOffsets::create();
    UInt32 timestamp_scale = 0;
    std::string_view metric_name;
};

Block makeTimeSeriesBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_metadata_rows,
    const StorageInMemoryMetadata & metadata,
    const String & samples_column_name)
{
    TimeSeriesBlockBuilder builder(time_series.size() + num_metadata_rows, metadata, samples_column_name);
    for (const auto & element : time_series)
    {
        for (const auto & label : element.labels())
            builder.addLabel(label.name(), label.value());
        for (const auto & sample : element.samples())
            builder.addSample(sample.timestamp(), sample.value());
        builder.finishTimeSeries(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS);
    }
    return builder.finish(num_metadata_rows, samples_column_name);
}

struct MetricsMetadata
{
    std::string_view metric_family_name;
    std::string_view type;
    std::string_view unit;
    std::string_view help;
};

Block makeMetricsMetadataBlock(
    const std::vector<MetricsMetadata> & metrics_metadata,
    size_t num_time_series_rows,
    const StorageInMemoryMetadata & metadata)
{
    const size_t num_rows = num_time_series_rows + metrics_metadata.size();

    const auto metric_family_type = metadata.columns.get(TimeSeriesColumnNames::MetricFamily).type;
    const auto type_type = metadata.columns.get(TimeSeriesColumnNames::Type).type;
    const auto unit_type = metadata.columns.get(TimeSeriesColumnNames::Unit).type;
    const auto help_type = metadata.columns.get(TimeSeriesColumnNames::Help).type;
    auto metric_family_column = metric_family_type->createColumn();
    auto type_column = type_type->createColumn();
    auto unit_column = unit_type->createColumn();
    auto help_column = help_type->createColumn();
    metric_family_column->reserve(num_rows);
    type_column->reserve(num_rows);
    unit_column->reserve(num_rows);
    help_column->reserve(num_rows);

    metric_family_column->insertManyDefaults(num_time_series_rows);
    type_column->insertManyDefaults(num_time_series_rows);
    unit_column->insertManyDefaults(num_time_series_rows);
    help_column->insertManyDefaults(num_time_series_rows);

    for (const auto & element : metrics_metadata)
    {
        metric_family_column->insertData(element.metric_family_name.data(), element.metric_family_name.size());
        type_column->insertData(element.type.data(), element.type.size());
        unit_column->insertData(element.unit.data(), element.unit.size());
        help_column->insertData(element.help.data(), element.help.size());
    }

    Block block;
    block.insert(ColumnWithTypeAndName{std::move(metric_family_column), metric_family_type, TimeSeriesColumnNames::MetricFamily});
    block.insert(ColumnWithTypeAndName{std::move(type_column), type_type, TimeSeriesColumnNames::Type});
    block.insert(ColumnWithTypeAndName{std::move(unit_column), unit_type, TimeSeriesColumnNames::Unit});
    block.insert(ColumnWithTypeAndName{std::move(help_column), help_type, TimeSeriesColumnNames::Help});
    return block;
}

void appendBlock(Block & block, Block block_to_append)
{
    for (auto & column : block_to_append)
        block.insert(std::move(column));
}

Block makeBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata,
    const StorageInMemoryMetadata & metadata,
    const String & samples_column_name)
{
    Block block;
    if (!time_series.empty())
    {
        appendBlock(
            block,
            makeTimeSeriesBlock(time_series, metrics_metadata.size(), metadata, samples_column_name));
    }
    if (!metrics_metadata.empty())
    {
        std::vector<MetricsMetadata> converted_metadata;
        converted_metadata.reserve(metrics_metadata.size());
        for (const auto & element : metrics_metadata)
            converted_metadata.emplace_back(
                element.metric_family_name(), metricTypeToString(element.type()), element.unit(), element.help());
        appendBlock(
            block,
            makeMetricsMetadataBlock(converted_metadata, time_series.size(), metadata));
    }
    return block;
}

size_t countFloatTimeSeries(const io::prometheus::write::v2::Request & request)
{
    size_t count = 0;
    for (const auto & element : request.timeseries())
        count += !element.samples().empty();
    return count;
}

Block makeBlock(
    const io::prometheus::write::v2::Request & request,
    const StorageInMemoryMetadata & metadata,
    const String & samples_column_name)
{
    const auto num_time_series = countFloatTimeSeries(request);
    const auto & symbols = request.symbols();
    const auto lookup = [&](UInt32 ref) -> const std::string &
    {
        if (ref >= static_cast<UInt32>(symbols.size()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Prometheus remote write v2 symbol reference {}", ref);
        return symbols[static_cast<int>(ref)];
    };

    std::vector<MetricsMetadata> metrics_metadata;
    for (const auto & element : request.timeseries())
    {
        if (!element.has_metadata())
            continue;
        if (element.labels_refs_size() % 2 != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus remote write v2 labels_refs size must be even");

        std::string_view metric_name;
        for (int i = 0; i < element.labels_refs_size(); i += 2)
        {
            const auto & name = lookup(element.labels_refs(i));
            const auto & value = lookup(element.labels_refs(i + 1));
            if (name == TimeSeriesTagNames::MetricName)
                metric_name = value;
        }

        const auto & element_metadata = element.metadata();
        metrics_metadata.emplace_back(
            metric_name,
            metricTypeToString(element_metadata.type()),
            lookup(element_metadata.unit_ref()),
            lookup(element_metadata.help_ref()));
    }
    if (!num_time_series && metrics_metadata.empty())
        return {};

    Block block;
    if (num_time_series)
    {
        TimeSeriesBlockBuilder builder(num_time_series + metrics_metadata.size(), metadata, samples_column_name);
        for (const auto & element : request.timeseries())
        {
            if (element.samples().empty())
                continue;
            if (element.labels_refs_size() % 2 != 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus remote write v2 labels_refs size must be even");

            for (int i = 0; i < element.labels_refs_size(); i += 2)
                builder.addLabel(lookup(element.labels_refs(i)), lookup(element.labels_refs(i + 1)));
            for (const auto & sample : element.samples())
                builder.addSample(sample.timestamp(), sample.value());
            builder.finishTimeSeries(ErrorCodes::BAD_ARGUMENTS);
        }
        appendBlock(block, builder.finish(metrics_metadata.size(), samples_column_name));
    }
    if (!metrics_metadata.empty())
        appendBlock(block, makeMetricsMetadataBlock(metrics_metadata, num_time_series, metadata));
    return block;
}

void insertBlock(Block block, StorageTimeSeries & storage, const ContextMutablePtr & context)
{
    if (!block.rows())
        return;

    auto insert_query = make_intrusive<ASTInsertQuery>();
    insert_query->table_id = storage.getStorageID();
    insert_query->format = "Native";

    auto columns_ast = make_intrusive<ASTExpressionList>();
    for (const auto & name : block.getNames())
        columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
    insert_query->columns = columns_ast;

    auto * queue = context->tryGetAsynchronousInsertQueue();
    const bool async_insert = queue && context->getSettingsRef()[Setting::async_insert];

    auto [ast, io] = executeQuery(insert_query->formatWithSecretsOneLine(), context);
    try
    {
        if (async_insert)
        {
            auto result = queue->pushQueryWithBlock(ast, std::move(block), context);
            if (result.status != AsynchronousInsertQueue::PushResult::OK)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result of pushing a block to the asynchronous insert queue");

            io.resetPipeline(/*cancel=*/ true);

            /// `ASYNC_INSERT_FLUSH_TIMEOUT` is returned to the client as HTTP 503: the remote-write protocol
            /// treats 4xx statuses (other than 429) as permanent failures and drops the data without a retry,
            /// while the data here is still in the queue and its fate is unknown, so the status must be retryable.
            const auto timeout = saturatedMilliseconds(context->getSettingsRef()[Setting::wait_for_async_insert_timeout].totalMilliseconds());
            if (result.future.wait_for(timeout) == std::future_status::timeout)
                throw Exception(ErrorCodes::ASYNC_INSERT_FLUSH_TIMEOUT, "Wait for asynchronous insert timeout ({} ms) exceeded", timeout.count());

            const auto progress = result.future.get();
            if (auto process_list_element = context->getProcessListElement())
            {
                process_list_element->updateProgressIn(Progress(ReadProgress(progress.rows, progress.bytes)));
                process_list_element->updateProgressOut(Progress(WriteProgress(progress.rows, progress.bytes)));
            }
        }
        else
        {
            PushingPipelineExecutor executor(io.pipeline);
            executor.start();
            executor.push(std::move(block));
            executor.finish();
        }
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    finishExecutedQuery(io, {});
}

}


PrometheusRemoteWriteProtocol::PrometheusRemoteWriteProtocol(
    StoragePtr time_series_storage_, const ContextMutablePtr & context_)
    : WithMutableContext(context_)
    , time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , log(getLogger("PrometheusRemoteWriteProtocol"))
{
    checkTimeSeriesVersionIsWritable(*time_series_storage);
}

PrometheusRemoteWriteProtocol::~PrometheusRemoteWriteProtocol() = default;


void PrometheusRemoteWriteProtocol::write(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    const auto storage_id = time_series_storage->getStorageID();
    LOG_TRACE(
        log,
        "{}: Writing {} time series and {} metrics metadata",
        storage_id.getNameForLogs(),
        time_series.size(),
        metrics_metadata.size());

    auto metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    const auto * samples_column_name = TimeSeriesColumnNames::getOuterSamples(time_series_storage->getVersion());
    insertBlock(makeBlock(time_series, metrics_metadata, *metadata, samples_column_name), *time_series_storage, getContext());

    LOG_TRACE(
        log,
        "{}: {} time series and {} metrics metadata written",
        storage_id.getNameForLogs(),
        time_series.size(),
        metrics_metadata.size());
}

size_t PrometheusRemoteWriteProtocol::write(const io::prometheus::write::v2::Request & request)
{
    size_t samples_written = 0;
    for (const auto & element : request.timeseries())
    {
        if (element.exemplars_size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus remote write v2 exemplars are not supported");
        if (element.histograms_size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus remote write v2 native histograms are not supported");
        if (element.samples().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus remote write v2 time series must contain samples");
        for (const auto & sample : element.samples())
        {
            if (sample.start_timestamp())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus remote write v2 sample start timestamps are not supported");
            ++samples_written;
        }
    }

    const auto storage_id = time_series_storage->getStorageID();
    const auto num_time_series = countFloatTimeSeries(request);
    LOG_TRACE(
        log,
        "{}: Writing {} time series",
        storage_id.getNameForLogs(),
        num_time_series);

    auto metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    const auto * samples_column_name = TimeSeriesColumnNames::getOuterSamples(time_series_storage->getVersion());
    insertBlock(makeBlock(request, *metadata, samples_column_name), *time_series_storage, getContext());

    LOG_TRACE(
        log,
        "{}: {} time series written",
        storage_id.getNameForLogs(),
        num_time_series);
    return samples_written;
}

}

#endif
