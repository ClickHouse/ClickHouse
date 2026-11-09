#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Common/ProfileEvents.h>
#include <Common/UTF8Helpers.h>
#include <Common/logger_useful.h>
#include <Common/saturatedDuration.h>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
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
#include <Storages/TimeSeries/checkTimeSeriesVersion.h>
#include <Storages/TimeSeries/getPrometheusLabelsHash.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>

#include <chrono>


namespace ProfileEvents
{
    extern const Event PrometheusRemoteWriteOutOfOrderExemplars;
}

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

void insertTimestamp(Int64 timestamp_ms, UInt32 scale, IColumn & column)
{
    if (typeid_cast<ColumnDecimal<DateTime64> *>(&column))
        column.insert(DecimalUtils::convertTo<DateTime64>(scale, DateTime64{timestamp_ms}, 3));
    else
        column.insert(DecimalUtils::convertTo<UInt32>(DateTime64{timestamp_ms}, 3));
}

/// An exemplar as Prometheus stores it: the labels are sorted by name and have no empty values.
struct Exemplar
{
    std::vector<std::pair<std::string_view, std::string_view>> labels;
    Float64 value = 0;
    Int64 timestamp = 0;
};

/// The longest exemplar label set Prometheus accepts, in code points of the names and values (`exemplar.ExemplarMaxLabelSetLength`).
constexpr size_t EXEMPLAR_MAX_LABEL_SET_LENGTH = 128;

/// How far into the future an exemplar can be (`maxAheadTime` of storage/remote/write_handler.go).
constexpr Int64 EXEMPLAR_MAX_AHEAD_TIME_MS = 10 * 60 * 1000;

/// Returns the exemplars of a time series which Prometheus 3.5 stores, in order. Like `AppendExemplar` it drops the empty labels,
/// the exemplars too far in the future and with too long labels, and a duplicate of the newest one or an exemplar older than it.
std::vector<Exemplar> getAcceptedExemplars(const prometheus::TimeSeries & element, Int64 max_timestamp, size_t & num_out_of_order)
{
    std::vector<Exemplar> accepted;
    for (const auto & proto_exemplar : element.exemplars())
    {
        Exemplar exemplar;
        exemplar.value = proto_exemplar.value();
        exemplar.timestamp = proto_exemplar.timestamp();
        if (exemplar.timestamp > max_timestamp)
            continue;

        size_t labels_length = 0;
        for (const auto & label : proto_exemplar.labels())
        {
            if (label.value().empty())
                continue;
            labels_length += UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(label.name().data()), label.name().size());
            labels_length += UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(label.value().data()), label.value().size());
            exemplar.labels.emplace_back(label.name(), label.value());
        }
        if (labels_length > EXEMPLAR_MAX_LABEL_SET_LENGTH)
            continue;
        std::sort(exemplar.labels.begin(), exemplar.labels.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

        if (!accepted.empty())
        {
            /// A zero timestamp means the exemplar has no timestamp, then only the labels and the value are compared.
            const auto & newest = accepted.back();
            bool has_timestamp = (exemplar.timestamp != 0) || (newest.timestamp != 0);
            if ((exemplar.labels == newest.labels) && (exemplar.value == newest.value)
                && (!has_timestamp || (exemplar.timestamp == newest.timestamp)))
                continue;

            if ((exemplar.timestamp < newest.timestamp)
                || ((exemplar.timestamp == newest.timestamp) && (exemplar.value < newest.value))
                || ((exemplar.timestamp == newest.timestamp) && (exemplar.value == newest.value)
                    && (getPrometheusLabelsHash(exemplar.labels) < getPrometheusLabelsHash(newest.labels))))
            {
                ++num_out_of_order;
                continue;
            }
        }

        accepted.push_back(std::move(exemplar));
    }
    return accepted;
}

Block makeTimeSeriesBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_metadata_rows,
    const StorageInMemoryMetadata & metadata,
    const String & samples_column_name,
    size_t & num_out_of_order_exemplars)
{
    const size_t num_rows = time_series.size() + num_metadata_rows;

    const auto metric_name_type = metadata.columns.get(TimeSeriesColumnNames::MetricName).type;
    auto metric_name_column = metric_name_type->createColumn();
    metric_name_column->reserve(num_rows);

    const auto tags_type = typeid_cast<std::shared_ptr<const DataTypeMap>>(metadata.columns.get(TimeSeriesColumnNames::Tags).type);
    if (!tags_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have a Map type", TimeSeriesColumnNames::Tags);
    auto tags_names = tags_type->getKeyType()->createColumn();
    auto tags_values = tags_type->getValueType()->createColumn();
    auto tags_offsets = ColumnArray::ColumnOffsets::create();
    tags_offsets->reserve(num_rows);

    const auto time_series_type
        = typeid_cast<std::shared_ptr<const DataTypeArray>>(metadata.columns.get(samples_column_name).type);
    if (!time_series_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have an Array type", samples_column_name);
    auto [timestamp_type, value_type] = splitTimeSeriesType(time_series_type);
    auto timestamps = timestamp_type->createColumn();
    auto values = value_type->createColumn();
    auto time_series_offsets = ColumnArray::ColumnOffsets::create();
    time_series_offsets->reserve(num_rows);
    const UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);

    /// The outer column `exemplars` exists only if the table stores exemplars, otherwise they are dropped.
    DataTypePtr exemplars_type;
    MutableColumnPtr exemplar_timestamps;
    MutableColumnPtr exemplar_values;
    MutableColumnPtr exemplar_label_names;
    MutableColumnPtr exemplar_label_values;
    auto exemplar_labels_offsets = ColumnArray::ColumnOffsets::create();
    auto exemplars_offsets = ColumnArray::ColumnOffsets::create();
    Int64 max_exemplar_timestamp = 0;
    if (const auto * exemplars_column = metadata.columns.tryGet(TimeSeriesColumnNames::Exemplars))
    {
        exemplars_type = exemplars_column->type;
        const auto & element_types = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*exemplars_type).getNestedType()).getElements();
        exemplar_timestamps = element_types.at(0)->createColumn();
        exemplar_values = element_types.at(1)->createColumn();
        const auto & labels_type = typeid_cast<const DataTypeMap &>(*element_types.at(2));
        exemplar_label_names = labels_type.getKeyType()->createColumn();
        exemplar_label_values = labels_type.getValueType()->createColumn();
        exemplars_offsets->reserve(num_rows);
        max_exemplar_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count() + EXEMPLAR_MAX_AHEAD_TIME_MS;
    }

    for (const auto & element : time_series)
    {
        std::string_view metric_name;
        bool has_metric_name = false;
        for (const auto & label : element.labels())
        {
            if (!has_metric_name && label.name() == TimeSeriesTagNames::MetricName && !label.value().empty())
            {
                metric_name = label.value();
                has_metric_name = true;
            }
            else
            {
                tags_names->insertData(label.name().data(), label.name().size());
                tags_values->insertData(label.value().data(), label.value().size());
            }
        }
        if (metric_name.empty())
            throw Exception(
                ErrorCodes::ILLEGAL_TIME_SERIES_TAGS,
                "Metric name is missing: a time series has no `{}` label with a non-empty value",
                TimeSeriesTagNames::MetricName);
        metric_name_column->insertData(metric_name.data(), metric_name.size());
        tags_offsets->insert(tags_names->size());

        for (const auto & sample : element.samples())
        {
            insertTimestamp(sample.timestamp(), timestamp_scale, *timestamps);
            values->insert(sample.value());
        }
        time_series_offsets->insert(timestamps->size());

        if (exemplars_type)
        {
            for (const auto & exemplar : getAcceptedExemplars(element, max_exemplar_timestamp, num_out_of_order_exemplars))
            {
                insertTimestamp(exemplar.timestamp, timestamp_scale, *exemplar_timestamps);
                exemplar_values->insert(exemplar.value);
                for (const auto & [name, value] : exemplar.labels)
                {
                    exemplar_label_names->insertData(name.data(), name.size());
                    exemplar_label_values->insertData(value.data(), value.size());
                }
                exemplar_labels_offsets->insert(exemplar_label_names->size());
            }
            exemplars_offsets->insert(exemplar_timestamps->size());
        }
    }

    metric_name_column->insertManyDefaults(num_metadata_rows);
    for (size_t i = 0; i != num_metadata_rows; ++i)
    {
        tags_offsets->insert(tags_names->size());
        time_series_offsets->insert(timestamps->size());
        if (exemplars_type)
            exemplars_offsets->insert(exemplar_timestamps->size());
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

    if (exemplars_type)
    {
        Columns label_columns;
        label_columns.push_back(std::move(exemplar_label_names));
        label_columns.push_back(std::move(exemplar_label_values));
        Columns exemplar_columns;
        exemplar_columns.push_back(std::move(exemplar_timestamps));
        exemplar_columns.push_back(std::move(exemplar_values));
        exemplar_columns.push_back(ColumnMap::create(
            ColumnArray::create(ColumnTuple::create(std::move(label_columns)), std::move(exemplar_labels_offsets))));
        auto exemplars_column = ColumnArray::create(ColumnTuple::create(std::move(exemplar_columns)), std::move(exemplars_offsets));
        block.insert(ColumnWithTypeAndName{std::move(exemplars_column), exemplars_type, TimeSeriesColumnNames::Exemplars});
    }
    return block;
}

Block makeMetricsMetadataBlock(
    const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata,
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
        const auto metric_type = metricTypeToString(element.type());
        metric_family_column->insertData(element.metric_family_name().data(), element.metric_family_name().size());
        type_column->insertData(metric_type.data(), metric_type.size());
        unit_column->insertData(element.unit().data(), element.unit().size());
        help_column->insertData(element.help().data(), element.help().size());
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
    const String & samples_column_name,
    size_t & num_out_of_order_exemplars)
{
    Block block;
    if (!time_series.empty())
    {
        appendBlock(
            block,
            makeTimeSeriesBlock(time_series, metrics_metadata.size(), metadata, samples_column_name, num_out_of_order_exemplars));
    }
    if (!metrics_metadata.empty())
    {
        appendBlock(
            block,
            makeMetricsMetadataBlock(metrics_metadata, time_series.size(), metadata));
    }
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

    /// The `INSERT` text is generated by the server, so it is parsed as ClickHouse SQL without parser limits,
    /// regardless of the `dialect` and the parser limits a request may set through URL parameters.
    auto [ast, io] = executeQuery(
        insert_query->formatWithSecretsOneLine(), context, QueryFlags{.parse_server_owned_query_without_limits = true});
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
    size_t num_out_of_order_exemplars = 0;
    insertBlock(makeBlock(time_series, metrics_metadata, *metadata, samples_column_name, num_out_of_order_exemplars),
                *time_series_storage, getContext());

    /// Like Prometheus, the out-of-order exemplars are counted and dropped without failing the request.
    if (num_out_of_order_exemplars)
    {
        ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteOutOfOrderExemplars, num_out_of_order_exemplars);
        LOG_WARNING(log, "{}: Dropped {} out-of-order exemplars", storage_id.getNameForLogs(), num_out_of_order_exemplars);
    }

    LOG_TRACE(
        log,
        "{}: {} time series and {} metrics metadata written",
        storage_id.getNameForLogs(),
        time_series.size(),
        metrics_metadata.size());
}

}

#endif
