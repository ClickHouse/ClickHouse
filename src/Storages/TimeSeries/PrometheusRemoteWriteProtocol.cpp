#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>

#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int THERE_IS_NO_COLUMN;
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

/// Returns true if `value` is the Prometheus "stale marker": the specific NaN payload
/// (`math.Float64frombits(0x7ff0000000000002)` in Prometheus's own Go code) that scrapers and
/// remote-write clients use to mark a series as stale (e.g. a scrape target disappeared). Real
/// Prometheus's query engine (`value.IsStaleNaN`) recognizes exactly this bit pattern and treats any
/// sample carrying it as if the series had no sample at that point ("absent"), never as a literal NaN
/// datapoint flowing into aggregations - unlike an ordinary user-supplied NaN, which must still
/// propagate as NaN.
///
/// This must be checked here, on the protobuf's `double`, and the result carried in the
/// `is_stale_marker` column: a "samples" table declaring `value Float32` narrows the marker to the same
/// canonical quiet NaN (0x7fc00000) as any other NaN, so it is no longer recognizable after that point.
bool isPrometheusStaleMarker(Float64 value)
{
    return std::bit_cast<UInt64>(value) == 0x7FF0000000000002ULL;
}

Block makeTimeSeriesBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_metadata_rows,
    const StorageInMemoryMetadata & metadata)
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

    const auto is_stale_marker_type
        = typeid_cast<std::shared_ptr<const DataTypeArray>>(metadata.columns.get(TimeSeriesColumnNames::IsStaleMarker).type);
    if (!is_stale_marker_type)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column `{}` must have an Array type", TimeSeriesColumnNames::IsStaleMarker);
    auto is_stale_markers = is_stale_marker_type->getNestedType()->createColumn();

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
            is_stale_markers->insert(isPrometheusStaleMarker(sample.value()) ? 1u : 0u);
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

    /// The flags are parallel to the samples, so both arrays use the same offsets.
    auto is_stale_marker_column = ColumnArray::create(std::move(is_stale_markers), time_series_offsets->clone());

    Columns time_series_tuple_columns;
    time_series_tuple_columns.push_back(std::move(timestamps));
    time_series_tuple_columns.push_back(std::move(values));
    auto time_series_column = ColumnArray::create(
        ColumnTuple::create(std::move(time_series_tuple_columns)), std::move(time_series_offsets));

    Block block;
    block.insert(ColumnWithTypeAndName{std::move(metric_name_column), metric_name_type, TimeSeriesColumnNames::MetricName});
    block.insert(ColumnWithTypeAndName{std::move(tags_column), tags_type, TimeSeriesColumnNames::Tags});
    block.insert(ColumnWithTypeAndName{std::move(time_series_column), time_series_type, TimeSeriesColumnNames::TimeSeries});
    block.insert(ColumnWithTypeAndName{std::move(is_stale_marker_column), is_stale_marker_type, TimeSeriesColumnNames::IsStaleMarker});
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
    const StorageInMemoryMetadata & metadata)
{
    Block block;
    if (!time_series.empty())
    {
        appendBlock(
            block,
            makeTimeSeriesBlock(time_series, metrics_metadata.size(), metadata));
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

    auto [ast, io] = executeQuery(insert_query->formatWithSecretsOneLine(), context);
    try
    {
        PushingPipelineExecutor executor(io.pipeline);
        executor.start();
        executor.push(std::move(block));
        executor.finish();
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

    /// The Prometheus remote-write protocol relies on the `is_stale_marker` column of the "samples" table to
    /// flag Prometheus stale markers instead of writing them as ordinary NaN samples (see
    /// `isPrometheusStaleMarker()` above). A "samples" table predating that column (or an
    /// external table using the old 3-column schema, which `normalizeTimeSeriesDefinition.cpp` still accepts
    /// for other purposes) cannot represent that flag, so writing to it here would silently store raw
    /// stale-NaN samples that are later read back as ordinary samples. Fail closed instead of doing that.
    if (!time_series.empty())
    {
        auto samples_table = time_series_storage->getTargetTable(ViewTarget::Samples, getContext());
        auto samples_table_metadata = samples_table->getInMemoryMetadataPtr(getContext(), false);
        if (!samples_table_metadata->columns.has(TimeSeriesColumnNames::IsStaleMarker))
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
                "{}: the \"samples\" table {} is missing column {} required for Prometheus stale-marker handling. "
                "Run ALTER TABLE {} ADD COLUMN {} UInt8, or recreate the TimeSeries table, to add it. "
                "Adding the column marks all existing rows non-stale, so a table which already holds stale markers "
                "needs a backfill too: with a Float64 \"value\" run "
                "ALTER TABLE {} UPDATE {} = 1 WHERE reinterpretAsUInt64(value) = 0x7FF0000000000002; "
                "with a Float32 \"value\" the marker's NaN payload was already lost at insert, "
                "so such history can only be corrected by re-ingesting it",
                storage_id.getNameForLogs(),
                samples_table->getStorageID().getNameForLogs(),
                TimeSeriesColumnNames::IsStaleMarker,
                samples_table->getStorageID().getNameForLogs(),
                TimeSeriesColumnNames::IsStaleMarker,
                samples_table->getStorageID().getNameForLogs(),
                TimeSeriesColumnNames::IsStaleMarker);
    }

    auto metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    insertBlock(makeBlock(time_series, metrics_metadata, *metadata), *time_series_storage, getContext());

    LOG_TRACE(
        log,
        "{}: {} time series and {} metrics metadata written",
        storage_id.getNameForLogs(),
        time_series.size(),
        metrics_metadata.size());
}

}

#endif
