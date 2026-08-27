#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>
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
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>

#include <bit>
#include <chrono>


namespace ProfileEvents
{
    extern const Event PrometheusRemoteWriteHistograms;
    extern const Event PrometheusRemoteWriteDroppedHistograms;
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
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
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

/// Sums the lengths of bucket spans, checking that each span is sane.
size_t getTotalSpanLength(const google::protobuf::RepeatedPtrField<prometheus::BucketSpan> & spans, std::string_view what)
{
    size_t total = 0;
    for (const auto & span : spans)
        total += span.length();
    if (total > 1000000)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has too many {} buckets: {}", what, total);
    return total;
}

/// Appends decoded bucket values (absolute counts) of one direction of a native histogram.
/// Int histograms carry deltas which are decoded to absolutes here; float histograms carry absolutes.
/// The absolute counts are also appended verbatim to `out_int_values`, which provides an exact
/// carrier for integers above 2^53 (where Float64 loses precision); that column stays empty
/// for the rows of float histograms, whose counts are fractional by design.
void appendHistogramBuckets(
    const google::protobuf::RepeatedPtrField<prometheus::BucketSpan> & spans,
    const google::protobuf::RepeatedField<Int64> & deltas,
    const google::protobuf::RepeatedField<double> & counts,
    bool is_float,
    std::string_view what,
    ColumnInt32 & out_span_offsets,
    ColumnUInt32 & out_span_lengths,
    ColumnArray::ColumnOffsets & out_spans_offsets,
    ColumnFloat64 & out_values,
    ColumnArray::ColumnOffsets & out_values_offsets,
    ColumnUInt64 & out_int_values,
    ColumnArray::ColumnOffsets & out_int_values_offsets)
{
    size_t total_span_length = getTotalSpanLength(spans, what);
    size_t num_values = is_float ? counts.size() : deltas.size();
    if (total_span_length != num_values)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Native histogram has {} {} bucket values but its spans cover {} buckets",
            num_values, what, total_span_length);

    for (const auto & span : spans)
    {
        out_span_offsets.insertValue(span.offset());
        out_span_lengths.insertValue(span.length());
    }
    out_spans_offsets.insertValue(out_span_offsets.size());

    if (is_float)
    {
        for (double count : counts)
        {
            /// The int flavor is checked after delta decoding below; the float one arrives verbatim.
            if (count < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has a negative {} bucket count: {}", what, count);
            out_values.insertValue(count);
        }
    }
    else
    {
        Int64 running = 0;
        for (Int64 delta : deltas)
        {
            Int64 new_running = 0;
            if (__builtin_add_overflow(running, delta, &new_running))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has an overflowing {} bucket count during delta decoding", what);
            running = new_running;
            if (running < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has a negative {} bucket count after delta decoding: {}", what, running);
            out_values.insertValue(static_cast<Float64>(running));
            out_int_values.insertValue(static_cast<UInt64>(running));
        }
    }
    out_values_offsets.insertValue(out_values.size());
    out_int_values_offsets.insertValue(out_int_values.size());
}

/// Returns true if `value` carries the Prometheus stale-marker NaN payload.
bool isPrometheusStaleMarker(Float64 value)
{
    return std::bit_cast<UInt64>(value) == 0x7FF0000000000002ULL;
}

/// Builds the outer `histograms` column: an array of histogram tuples per time-series row.
/// The tuple element order matches TimeSeriesHistogramsTupleIndex. Reusable for future
/// remote-write 2.0 messages, which carry the same Histogram submessage.
ColumnPtr makeHistogramsColumn(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_trailing_default_rows,
    const DataTypePtr & timestamp_type,
    size_t & out_num_histograms)
{
    auto timestamps = timestamp_type->createColumn();
    const UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);

    auto flags = ColumnUInt8::create();
    auto schemas = ColumnInt8::create();
    auto zero_thresholds = ColumnFloat64::create();
    auto counts = ColumnFloat64::create();
    auto sums = ColumnFloat64::create();
    auto zero_counts = ColumnFloat64::create();

    /// Exact integer carriers of the counts of an integer-flavor histogram; see TimeSeriesHistogramsTupleIndex.
    auto counts_int = ColumnUInt64::create();
    auto zero_counts_int = ColumnUInt64::create();

    auto make_spans_column = []
    {
        return std::tuple{ColumnInt32::create(), ColumnUInt32::create(), ColumnArray::ColumnOffsets::create()};
    };
    auto [positive_span_offsets, positive_span_lengths, positive_spans_offsets] = make_spans_column();
    auto [negative_span_offsets, negative_span_lengths, negative_spans_offsets] = make_spans_column();
    auto positive_values = ColumnFloat64::create();
    auto positive_values_offsets = ColumnArray::ColumnOffsets::create();
    auto negative_values = ColumnFloat64::create();
    auto negative_values_offsets = ColumnArray::ColumnOffsets::create();
    auto custom_values = ColumnFloat64::create();
    auto custom_values_offsets = ColumnArray::ColumnOffsets::create();
    auto positive_int_values = ColumnUInt64::create();
    auto positive_int_values_offsets = ColumnArray::ColumnOffsets::create();
    auto negative_int_values = ColumnUInt64::create();
    auto negative_int_values_offsets = ColumnArray::ColumnOffsets::create();

    auto histograms_offsets = ColumnArray::ColumnOffsets::create();

    size_t num_histograms = 0;
    for (const auto & element : time_series)
    {
        for (const auto & histogram : element.histograms())
        {
            /// Float-ness is decided solely by the `count` oneof, matching upstream Prometheus
            /// (prompb/codec.go `IsFloatHistogram`). The two oneofs (`count`, `zero_count`) are independent
            /// and the bucket lists are plain repeated fields, so a spec-invalid mix is wire-representable;
            /// reject it rather than silently reading the unset arm (which would store zeroes).
            bool is_float = histogram.count_case() == prometheus::Histogram::kCountFloat;
            bool zero_count_is_float = histogram.zero_count_case() == prometheus::Histogram::kZeroCountFloat;
            if (is_float != zero_count_is_float)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram mixes an {} count with an {} zero count",
                    is_float ? "float" : "int", zero_count_is_float ? "float" : "int");
            if (is_float
                && (!histogram.positive_deltas().empty() || !histogram.negative_deltas().empty()))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram is a float histogram but carries integer bucket deltas");
            if (!is_float
                && (!histogram.positive_counts().empty() || !histogram.negative_counts().empty()))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram is an integer histogram but carries float bucket counts");

            insertTimestamp(histogram.timestamp(), timestamp_scale, *timestamps);

            Float64 count = is_float ? histogram.count_float() : static_cast<Float64>(histogram.count_int());
            Float64 zero_count = is_float ? histogram.zero_count_float() : static_cast<Float64>(histogram.zero_count_int());
            Float64 sum = histogram.sum();

            /// Only the float arms can be negative: the int ones are unsigned on the wire. NaN
            /// compares false here, so a stale marker still gets through.
            if (count < 0 || zero_count < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has a negative {}: {}",
                    count < 0 ? "count" : "zero count", count < 0 ? count : zero_count);

            if (histogram.reset_hint() < prometheus::Histogram::UNKNOWN || histogram.reset_hint() > prometheus::Histogram::GAUGE)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has an unknown counter reset hint: {}", static_cast<int>(histogram.reset_hint()));
            if (histogram.schema() < -53 || histogram.schema() > 8)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has an out-of-range bucket schema: {}", histogram.schema());

            UInt8 histogram_flags = 0;
            if (is_float)
                histogram_flags |= TimeSeriesHistogramFlags::IsFloat;
            histogram_flags |= static_cast<UInt8>(histogram.reset_hint()) << TimeSeriesHistogramFlags::CounterResetHintShift;
            if (isPrometheusStaleMarker(sum))
                histogram_flags |= TimeSeriesHistogramFlags::StaleMarker;

            flags->insertValue(histogram_flags);
            schemas->insertValue(static_cast<Int8>(histogram.schema()));
            zero_thresholds->insertValue(histogram.zero_threshold());
            counts->insertValue(count);
            sums->insertValue(sum);
            zero_counts->insertValue(zero_count);

            /// The exact carriers stay zero/empty for float histograms: their counts are not integers.
            counts_int->insertValue(is_float ? 0 : histogram.count_int());
            zero_counts_int->insertValue(is_float ? 0 : histogram.zero_count_int());

            appendHistogramBuckets(
                histogram.positive_spans(), histogram.positive_deltas(), histogram.positive_counts(), is_float, "positive",
                *positive_span_offsets, *positive_span_lengths, *positive_spans_offsets,
                *positive_values, *positive_values_offsets,
                *positive_int_values, *positive_int_values_offsets);
            appendHistogramBuckets(
                histogram.negative_spans(), histogram.negative_deltas(), histogram.negative_counts(), is_float, "negative",
                *negative_span_offsets, *negative_span_lengths, *negative_spans_offsets,
                *negative_values, *negative_values_offsets,
                *negative_int_values, *negative_int_values_offsets);

            for (double custom_value : histogram.custom_values())
                custom_values->insertValue(custom_value);
            custom_values_offsets->insertValue(custom_values->size());

            ++num_histograms;
        }
        histograms_offsets->insertValue(flags->size());
    }

    for (size_t i = 0; i != num_trailing_default_rows; ++i)
        histograms_offsets->insertValue(flags->size());

    auto make_spans_array = [](auto && span_offsets, auto && span_lengths, auto && spans_offsets) -> ColumnPtr
    {
        Columns span_tuple_columns;
        span_tuple_columns.push_back(std::forward<decltype(span_offsets)>(span_offsets));
        span_tuple_columns.push_back(std::forward<decltype(span_lengths)>(span_lengths));
        return ColumnArray::create(ColumnTuple::create(std::move(span_tuple_columns)), std::forward<decltype(spans_offsets)>(spans_offsets));
    };

    Columns tuple_columns;
    tuple_columns.resize(TimeSeriesHistogramsTupleIndex::Size);
    tuple_columns[TimeSeriesHistogramsTupleIndex::Timestamp] = std::move(timestamps);
    tuple_columns[TimeSeriesHistogramsTupleIndex::Flags] = std::move(flags);
    tuple_columns[TimeSeriesHistogramsTupleIndex::Schema] = std::move(schemas);
    tuple_columns[TimeSeriesHistogramsTupleIndex::ZeroThreshold] = std::move(zero_thresholds);
    tuple_columns[TimeSeriesHistogramsTupleIndex::Count] = std::move(counts);
    tuple_columns[TimeSeriesHistogramsTupleIndex::Sum] = std::move(sums);
    tuple_columns[TimeSeriesHistogramsTupleIndex::ZeroCount] = std::move(zero_counts);
    tuple_columns[TimeSeriesHistogramsTupleIndex::PositiveSpans]
        = make_spans_array(std::move(positive_span_offsets), std::move(positive_span_lengths), std::move(positive_spans_offsets));
    tuple_columns[TimeSeriesHistogramsTupleIndex::PositiveValues]
        = ColumnArray::create(std::move(positive_values), std::move(positive_values_offsets));
    tuple_columns[TimeSeriesHistogramsTupleIndex::NegativeSpans]
        = make_spans_array(std::move(negative_span_offsets), std::move(negative_span_lengths), std::move(negative_spans_offsets));
    tuple_columns[TimeSeriesHistogramsTupleIndex::NegativeValues]
        = ColumnArray::create(std::move(negative_values), std::move(negative_values_offsets));
    tuple_columns[TimeSeriesHistogramsTupleIndex::CustomValues]
        = ColumnArray::create(std::move(custom_values), std::move(custom_values_offsets));
    tuple_columns[TimeSeriesHistogramsTupleIndex::CountInt] = std::move(counts_int);
    tuple_columns[TimeSeriesHistogramsTupleIndex::ZeroCountInt] = std::move(zero_counts_int);
    tuple_columns[TimeSeriesHistogramsTupleIndex::PositiveValuesInt]
        = ColumnArray::create(std::move(positive_int_values), std::move(positive_int_values_offsets));
    tuple_columns[TimeSeriesHistogramsTupleIndex::NegativeValuesInt]
        = ColumnArray::create(std::move(negative_int_values), std::move(negative_int_values_offsets));

    out_num_histograms = num_histograms;
    return ColumnArray::create(ColumnTuple::create(std::move(tuple_columns)), std::move(histograms_offsets));
}

Block makeTimeSeriesBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_metadata_rows,
    const StorageInMemoryMetadata & metadata,
    bool with_histograms)
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
        = typeid_cast<std::shared_ptr<const DataTypeArray>>(metadata.columns.get(TimeSeriesColumnNames::TimeSeries).type);
    if (!time_series_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have an Array type", TimeSeriesColumnNames::TimeSeries);
    auto [timestamp_type, value_type] = splitTimeSeriesType(time_series_type);
    auto timestamps = timestamp_type->createColumn();
    auto values = value_type->createColumn();
    auto time_series_offsets = ColumnArray::ColumnOffsets::create();
    time_series_offsets->reserve(num_rows);
    const UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_type).value_or(0);

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
    }

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
    block.insert(ColumnWithTypeAndName{std::move(time_series_column), time_series_type, TimeSeriesColumnNames::TimeSeries});

    if (with_histograms)
    {
        size_t num_histograms = 0;
        auto histograms_column = makeHistogramsColumn(time_series, num_metadata_rows, timestamp_type, num_histograms);
        block.insert(ColumnWithTypeAndName{
            histograms_column, metadata.columns.get(TimeSeriesColumnNames::Histograms).type, TimeSeriesColumnNames::Histograms});
        ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteHistograms, num_histograms);
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
    bool with_histograms)
{
    Block block;
    if (!time_series.empty())
    {
        appendBlock(
            block,
            makeTimeSeriesBlock(time_series, metrics_metadata.size(), metadata, with_histograms));
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

    auto [ast, io] = executeQuery(insert_query->formatWithSecretsOneLine(), context);
    try
    {
        if (async_insert)
        {
            auto result = queue->pushQueryWithBlock(ast, std::move(block), context);
            if (result.status != AsynchronousInsertQueue::PushResult::OK)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result of pushing a block to the asynchronous insert queue");

            io.resetPipeline(/*cancel=*/ true);

            const auto timeout_ms = context->getSettingsRef()[Setting::wait_for_async_insert_timeout].totalMilliseconds();
            if (result.future.wait_for(std::chrono::milliseconds(timeout_ms)) == std::future_status::timeout)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for asynchronous insert timeout ({} ms) exceeded", timeout_ms);

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

    size_t num_histograms = 0;
    for (const auto & element : time_series)
        num_histograms += element.histograms_size();

    /// Histogram samples can be stored only when the table was created with a "histograms" target;
    /// otherwise they are dropped, loudly, with a pointer at the migration.
    bool with_histograms = metadata->columns.has(TimeSeriesColumnNames::Histograms)
        && time_series_storage->hasTarget(ViewTarget::Histograms);
    if (num_histograms && !with_histograms)
    {
        /// Count the dropped histograms in both events: `PrometheusRemoteWriteHistograms` tracks
        /// everything received, and the difference with `PrometheusRemoteWriteDroppedHistograms`
        /// shows how many were actually stored.
        ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteHistograms, num_histograms);
        ProfileEvents::increment(ProfileEvents::PrometheusRemoteWriteDroppedHistograms, num_histograms);
        LOG_WARNING(LogFrequencyLimiter(log, 60),
            "{}: Dropping {} native histogram samples: the table has no \"histograms\" target table. "
            "Recreate the TimeSeries table with SETTINGS store_native_histograms = 1 to store them",
            storage_id.getNameForLogs(), num_histograms);
    }

    insertBlock(makeBlock(time_series, metrics_metadata, *metadata, with_histograms && num_histograms), *time_series_storage, getContext());

    LOG_TRACE(
        log,
        "{}: {} time series and {} metrics metadata written",
        storage_id.getNameForLogs(),
        time_series.size(),
        metrics_metadata.size());
}

}

#endif
