#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <cmath>
#include <limits>
#include <tuple>
#include <vector>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Core/Types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

namespace Setting
{
    extern const SettingsBool enable_materialized_cte;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
}

namespace
{
constexpr UInt32 LOOKBACK_DELTA_SCALE = 9;

Decimal64 parsePrometheusLookbackDelta(const String & value, UInt32 timestamp_scale)
{
    const auto high_precision_value = parseTimeSeriesDuration(value, LOOKBACK_DELTA_SCALE);
    if (high_precision_value <= 0 || timestamp_scale >= LOOKBACK_DELTA_SCALE)
        return high_precision_value;

    const auto divisor = DecimalUtils::scaleMultiplier<Decimal64>(LOOKBACK_DELTA_SCALE - timestamp_scale);
    auto timestamp_ticks = high_precision_value.value / divisor;
    if (high_precision_value.value % divisor)
        ++timestamp_ticks;

    return Decimal64{timestamp_ticks};
}

/// Makes a "SELECT [DISTINCT] <expressions> FROM (<subquery>) [LIMIT <limit>]" query.
ASTPtr makeSelectFromSubquery(ASTs select_list, ASTPtr subquery, bool distinct, std::optional<UInt64> limit)
{
    auto select_query = make_intrusive<ASTSelectQuery>();
    select_query->distinct = distinct;

    auto select_list_exp = make_intrusive<ASTExpressionList>();
    select_list_exp->children = std::move(select_list);
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));

    /// FROM (<subquery>)
    auto subquery_ast = make_intrusive<ASTSubquery>(std::move(subquery));
    auto table_exp = make_intrusive<ASTTableExpression>();
    table_exp->subquery = subquery_ast;
    table_exp->children.push_back(std::move(subquery_ast));

    auto table = make_intrusive<ASTTablesInSelectQueryElement>();
    table->table_expression = table_exp;
    table->children.push_back(std::move(table_exp));

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(std::move(table));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    if (limit)
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(*limit));

    /// Wrap the select query into ASTSelectWithUnionQuery, the form produced by the parser.
    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->children.push_back(std::move(list_of_selects));
    select_with_union_query->list_of_selects = select_with_union_query->children.back();
    return select_with_union_query;
}
}

PrometheusHTTPProtocolAPI::PrometheusHTTPProtocolAPI(ConstStoragePtr time_series_storage_, const ContextMutablePtr & context_)
    : WithMutableContext{context_}
    , time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , log(getLogger("PrometheusHTTPProtocolAPI"))
{
}

PrometheusHTTPProtocolAPI::~PrometheusHTTPProtocolAPI() = default;

void PrometheusHTTPProtocolAPI::executePromQLQuery(
    WriteBuffer & response,
    const Params & params,
    QueryFinishCallback query_finish_callback)
{
    PrometheusQueryEvaluationSettings evaluation_settings;
    evaluation_settings.time_series_storage_id = time_series_storage->getStorageID();
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    std::tie(evaluation_settings.timestamp_data_type, evaluation_settings.scalar_data_type)
        = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type);
    evaluation_settings.storage_has_native_histograms = time_series_storage->hasTarget(ViewTarget::Histograms);
    UInt32 timestamp_scale = tryGetDecimalScale(*evaluation_settings.timestamp_data_type).value_or(0);

    if (!params.lookback_delta_param.empty())
    {
        const auto lookback_delta = parsePrometheusLookbackDelta(params.lookback_delta_param, timestamp_scale);
        if (lookback_delta > 0)
            evaluation_settings.instant_selector_window = lookback_delta;
    }

    auto query_tree = std::make_shared<PrometheusQueryTree>();
    query_tree->parse(params.promql_query, timestamp_scale);
    LOG_TRACE(log, "Parsed PromQL query: {}. Result type: {}", params.promql_query, query_tree->getResultType());

    if (params.type == Type::Instant)
    {
        evaluation_settings.mode = PrometheusQueryEvaluationMode::QUERY;
        if (params.time_param.empty())
        {
            evaluation_settings.use_current_time = true;
        }
        else
        {
            evaluation_settings.start_time = parseTimeSeriesTimestamp(params.time_param, timestamp_scale);
            evaluation_settings.end_time = evaluation_settings.start_time;
            evaluation_settings.step = 0;
        }
    }
    else if (params.type == Type::Range)
    {
        evaluation_settings.mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        evaluation_settings.start_time = parseTimeSeriesTimestamp(params.start_param, timestamp_scale);
        evaluation_settings.end_time = parseTimeSeriesTimestamp(params.end_param, timestamp_scale);
        evaluation_settings.step = parseTimeSeriesDuration(params.step_param, timestamp_scale);
    }

    PrometheusQueryToSQL::Converter converter{query_tree, evaluation_settings};
    auto sql_query = converter.getSQL();

    chassert(sql_query);
    LOG_TRACE(log, "SQL query to execute:\n{}", sql_query->formatForLogging());

    /// The generated SQL relies on `AS MATERIALIZED` (see SQLSubqueryType::MATERIALIZED_TABLE), which takes effect only with `enable_materialized_cte`.
    /// Enable it unless the user set it explicitly.
    if (!getContext()->getSettingsRef()[Setting::enable_materialized_cte].changed)
        getContext()->setSetting("enable_materialized_cte", true);

    /// `AS MATERIALIZED` is honored by the analyzer only, so the generated SQL always runs the analyzer.
    getContext()->setSetting("allow_experimental_analyzer", true);

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Mind using the `getResultType` method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
        writeQueryResponse(response, executor, converter.getResultType());

        /// Store the buffered result in the query result cache now (no-op if no cache writers exist in the pipeline):
        /// the executor's destructor cancels the pipeline processors, after which the pending write would be discarded.
        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early so a slow client draining the response does not keep occupying it,
    /// then flush the response (query_finish_callback) and record QueryFinish.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::writeQueryResponse(
    WriteBuffer & response, PullingAsyncPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type)
{
    /// Pull until the first non-empty block before writing the header, because `pull` can throw and it's better to catch it early
    /// and write the correct error header {"status":"error", ...} in PrometheusRequestHandler::QueryImpl.
    bool has_output = false;
    Block block;
    while (pulling_executor.pull(block))
    {
        if (block.rows() > 0)
        {
            has_output = true;
            break;
        }
    }

    writeQueryResponseHeader(response, result_type);

    if (has_output)
    {
        /// `first_result` tracks whether any element was actually emitted: a block may emit zero elements (e.g. all rows are
        /// stale-marker histograms, which are skipped), and a comma written for such a block would produce malformed JSON.
        bool first_result = true;
        if (writeQueryResponseBlock(response, result_type, block, /*first=*/ first_result))
            first_result = false;

        while (pulling_executor.pull(block))
        {
            if (block.rows() > 0)
            {
                if (writeQueryResponseBlock(response, result_type, block, /*first=*/ first_result))
                    first_result = false;
            }
        }
    }

    writeQueryResponseFooter(response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseHeader(WriteBuffer & response, PrometheusQueryResultType result_type)
{
    std::string_view result_type_str;
    switch (result_type)
    {
        case PrometheusQueryTree::ResultType::SCALAR:
            result_type_str = "scalar";
            break;
        case PrometheusQueryTree::ResultType::STRING:
            result_type_str = "string";
            break;
        case PrometheusQueryTree::ResultType::INSTANT_VECTOR:
            result_type_str = "vector";
            break;
        case PrometheusQueryTree::ResultType::RANGE_VECTOR:
            result_type_str = "matrix";
            break;
    }
    chassert(!result_type_str.empty());
    writeString(R"({"status":"success","data":{"resultType":")", response);
    writeString(result_type_str, response);
    writeString(R"(","result":[)", response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseFooter(WriteBuffer & response)
{
    writeString("]}}", response);
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseBlock(WriteBuffer & response, PrometheusQueryResultType result_type, const Block & result_block, bool first)
{
    LOG_TRACE(log, "Prometheus: Writing {} result ({} rows)", result_type, result_block.rows());

    switch (result_type)
    {
        case PrometheusQueryTree::ResultType::SCALAR:
        {
            return writeQueryResponseScalarBlock(response, result_block, first);
        }
        case PrometheusQueryTree::ResultType::STRING:
        {
            return writeQueryResponseStringBlock(response, result_block, first);
        }
        case PrometheusQueryTree::ResultType::INSTANT_VECTOR:
        {
            return writeQueryResponseInstantVectorBlock(response, result_block, first);
        }
        case PrometheusQueryTree::ResultType::RANGE_VECTOR:
        {
            return writeQueryResponseRangeVectorBlock(response, result_block, first);
        }
    }
    UNREACHABLE();
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseScalarBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    if (!first || (result_block.rows() > 1))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query outputs multiple rows but expected to return a scalar");

    // Write timestamp
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    DateTime64 timestamp = timestamp_column->getInt(0);
    writeTimestamp(response, timestamp, timestamp_scale);

    writeString(",", response);

    // Write value
    const auto & scalar_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
    Float64 value = scalar_column->getFloat64(0);
    writeString("\"", response);
    writeScalar(response, value);
    writeString("\"", response);

    return true;
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseStringBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    if (!first || (result_block.rows() > 1))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query outputs multiple rows but expected to return a string");

    // Write timestamp
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    DateTime64 timestamp = timestamp_column->getInt(0);
    writeTimestamp(response, timestamp, timestamp_scale);

    writeString(",", response);

    // Write value
    const auto & string_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
    auto value = string_column->getDataAt(0);
    writeJSONString(value, response, format_settings);

    return true;
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseInstantVectorBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    if (const auto * histogram_column = result_block.findByName(String(TimeSeriesColumnNames::Histogram));
        histogram_column && isTimeSeriesHistogramTupleType(histogram_column->type))
    {
        return writeQueryResponseInstantVectorBlockWithHistograms(response, result_block, first);
    }

    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;

    bool need_comma = !first;
    bool emitted_any = false;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        if (need_comma)
            writeString(",", response);

        writeString("{", response);

        // Write metric labels
        writeString(R"("metric":)", response);
        writeTags(response, result_block, i);

        writeString(",", response);

        // Write value [timestamp, "value"]
        writeString("\"value\":[", response);

        // Write timestamp
        DateTime64 timestamp = timestamp_column->getInt(i);
        writeTimestamp(response, timestamp, timestamp_scale);

        writeString(",", response);

        // Write value
        Float64 value = value_column->getFloat64(i);
        writeString("\"", response);
        writeScalar(response, value);
        writeString("\"", response);

        writeString("]}", response);
        need_comma = true;
        emitted_any = true;
    }

    return emitted_any;
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    if (result_block.findByName(String(TimeSeriesColumnNames::HistogramSeries)))
    {
        return writeQueryResponseRangeVectorBlockWithHistograms(response, result_block, first);
    }

    const auto & time_series_column = result_block.getByName(TimeSeriesColumnNames::TimeSeries).column;
    const auto & array_column = typeid_cast<const ColumnArray &>(*time_series_column);
    const auto & offsets = array_column.getOffsets();
    const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
    const auto & timestamp_column = tuple_column.getColumn(0);
    const auto & value_column = tuple_column.getColumn(1);

    auto timestamp_data_type
        = typeid_cast<const DataTypeTuple &>(
              *typeid_cast<const DataTypeArray &>(*result_block.getByName(TimeSeriesColumnNames::TimeSeries).type).getNestedType())
              .getElement(0);

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    bool need_comma = !first;
    bool emitted_any = false;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        if (need_comma)
            writeString(",", response);

        writeString("{", response);

        // Write labels
        writeString(R"("metric":)", response);
        writeTags(response, result_block, i);
        writeString(",", response);

        // Extract time series data
        writeString(R"("values":[)", response);

        size_t start = (i == 0) ? 0 : offsets[i-1];
        size_t end = offsets[i];

        for (size_t j = start; j < end; ++j)
        {
            if (j > start)
                writeString(",", response);

            writeString("[", response);
            DateTime64 timestamp = timestamp_column.getInt(j);
            writeTimestamp(response, timestamp, timestamp_scale);
            writeString(",\"", response);
            Float64 value = value_column.getFloat64(j);
            writeScalar(response, value);
            writeString("\"]", response);
        }

        writeString("]}", response);
        need_comma = true;
        emitted_any = true;
    }

    return emitted_any;
}

struct PrometheusHTTPProtocolAPI::HistogramPayloadColumns
{
    const IColumn * flags;
    const IColumn * schema;
    const IColumn * zero_threshold;
    const IColumn * count;
    const IColumn * sum;
    const IColumn * zero_count;
    const IColumn * positive_spans;
    const IColumn * positive_values;
    const IColumn * negative_spans;
    const IColumn * negative_values;
    const IColumn * custom_values;
};

PrometheusHTTPProtocolAPI::HistogramPayloadColumns PrometheusHTTPProtocolAPI::resolveHistogramPayloadColumns(
    const DataTypePtr & payload_tuple_type, const IColumn & payload_tuple_column)
{
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(payload_tuple_type.get());
    const auto * tuple_column = typeid_cast<const ColumnTuple *>(&payload_tuple_column);
    if (!tuple_type || !tuple_column)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected a histogram payload tuple but got type `{}` and column `{}`",
            payload_tuple_type->getName(), payload_tuple_column.getName());

    const auto payload_tuple = std::static_pointer_cast<const DataTypeTuple>(getTimeSeriesHistogramPayloadTupleType());
    const auto & canonical_names = payload_tuple->getElementNames();

    /// A named tuple (produced by the `timeSeriesHistogram*ToGrid` aggregates) is resolved by name at any positions; an unnamed one
    /// (`tuple(...)` without `enable_named_columns_in_function_tuple`, e.g. StoreMethod::HISTOGRAM_RAW_DATA) carries the payload in the canonical order of `getTimeSeriesHistogramPayloadColumns`.
    if (!tuple_type->hasExplicitNames() && tuple_type->getElements().size() < TimeSeriesHistogramPayloadTupleIndex::Size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Histogram payload tuple of type `{}` has {} elements but at least {} are required",
            payload_tuple_type->getName(), tuple_type->getElements().size(), TimeSeriesHistogramPayloadTupleIndex::Size);

    const IColumn * element_columns[TimeSeriesHistogramPayloadTupleIndex::Size];

    for (size_t i = 0; i < TimeSeriesHistogramPayloadTupleIndex::Size; ++i)
    {
        size_t pos = i;
        if (tuple_type->hasExplicitNames())
        {
            const auto found = tuple_type->tryGetPositionByName(canonical_names[i]);
            if (!found)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Histogram payload tuple of type `{}` has no element `{}`", payload_tuple_type->getName(), canonical_names[i]);
            pos = *found;
        }
        if (!tuple_type->getElement(pos)->equals(*payload_tuple->getElement(i)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Histogram payload tuple of type `{}` has element `{}` of type `{}` but `{}` is required",
                payload_tuple_type->getName(), canonical_names[i], tuple_type->getElement(pos)->getName(), payload_tuple->getElement(i)->getName());

        element_columns[i] = &tuple_column->getColumn(pos);
    }

    return HistogramPayloadColumns{
        element_columns[TimeSeriesHistogramPayloadTupleIndex::Flags],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::Schema],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::ZeroThreshold],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::Count],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::Sum],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::ZeroCount],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::PositiveSpans],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::PositiveValues],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::NegativeSpans],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::NegativeValues],
        element_columns[TimeSeriesHistogramPayloadTupleIndex::CustomValues],
    };
}

void PrometheusHTTPProtocolAPI::writeHistogram(WriteBuffer & response, const HistogramPayloadColumns & payload, size_t row_index)
{
    const Int32 schema = static_cast<Int32>(payload.schema->getInt(row_index));
    const bool custom_buckets = (schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);
    if (!custom_buckets && (schema < HISTOGRAM_EXPONENTIAL_SCHEMA_MIN || schema > HISTOGRAM_EXPONENTIAL_SCHEMA_MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has an invalid bucket schema: {}", schema);

    const Float64 zero_threshold = payload.zero_threshold->getFloat64(row_index);
    const Float64 zero_count = payload.zero_count->getFloat64(row_index);

    /// The buckets to emit: (boundary rule, lower bound, upper bound, count). Boundary rules (Prometheus util/jsonutil/marshal.go):
    /// 0 = upper bound inclusive (positive exponential), 1 = lower bound inclusive (negative exponential), 3 = both (zero bucket and first custom bucket).
    std::vector<std::tuple<int, Float64, Float64, Float64>> buckets;

    auto add_bucket = [&](int rule, Float64 lower, Float64 upper, Float64 bucket_count)
    {
        /// No need to expose empty buckets in JSON.
        if (bucket_count == 0)
            return;
        /// A boundary overlapping the zero bucket is clamped to the zero threshold
        /// (mirrors `allFloatBucketIterator` in Prometheus model/histogram/float_histogram.go).
        if (upper < 0 && upper > -zero_threshold)
            upper = -zero_threshold;
        if (lower > 0 && lower < zero_threshold)
            lower = zero_threshold;
        buckets.emplace_back(rule, lower, upper, bucket_count);
    };

    if (custom_buckets)
    {
        /// Custom buckets exist on the positive side only; boundaries come from `custom_values` (mirrors `getBound` in Prometheus:
        /// -Inf below the first bound, +Inf above the last one, out-of-range bucket indices are rejected).
        const auto & custom_values_array = typeid_cast<const ColumnArray &>(*payload.custom_values);
        const auto & custom_values_offsets = custom_values_array.getOffsets();
        const size_t custom_values_begin = (row_index == 0) ? 0 : custom_values_offsets[row_index - 1];
        const size_t num_custom_values = custom_values_offsets[row_index] - custom_values_begin;
        const auto & custom_values_data = custom_values_array.getData();

        auto custom_bound = [&](Int64 idx) -> Float64
        {
            if (idx < 0)
                return -std::numeric_limits<Float64>::infinity();
            const auto uidx = static_cast<UInt64>(idx);
            if (uidx >= num_custom_values)
            {
                if (uidx == num_custom_values)
                    return std::numeric_limits<Float64>::infinity();
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Native histogram bucket index {} is out of bounds for {} custom bucket bounds",
                    idx, num_custom_values);
            }
            return custom_values_data.getFloat64(custom_values_begin + uidx);
        };

        /// Custom buckets are (lower, upper] throughout, so every one of them uses rule 0 - including
        /// the first, whose lower bound is -Inf and therefore cannot be inclusive.
        const auto positive_buckets = expandHistogramSpans(*payload.positive_spans, *payload.positive_values, row_index);
        for (const auto & bucket : positive_buckets)
            add_bucket(0, custom_bound(bucket.index - 1), custom_bound(bucket.index), bucket.count);
    }
    else
    {
        /// Negative buckets go first, from the most negative one up towards the zero bucket
        /// (the reverse of the span expansion order).
        const auto negative_buckets = expandHistogramSpans(*payload.negative_spans, *payload.negative_values, row_index);
        for (auto it = negative_buckets.rbegin(); it != negative_buckets.rend(); ++it)
            add_bucket(1, -getHistogramBoundExponential(it->index, schema), -getHistogramBoundExponential(it->index - 1, schema), it->count);

        if (zero_count > 0)
            add_bucket(3, -zero_threshold, zero_threshold, zero_count);

        const auto positive_buckets = expandHistogramSpans(*payload.positive_spans, *payload.positive_values, row_index);
        for (const auto & bucket : positive_buckets)
            add_bucket(0, getHistogramBoundExponential(bucket.index - 1, schema), getHistogramBoundExponential(bucket.index, schema), bucket.count);
    }

    writeString(R"({"count":")", response);
    writeScalar(response, payload.count->getFloat64(row_index));
    writeString(R"(","sum":")", response);
    writeScalar(response, payload.sum->getFloat64(row_index));
    writeString("\"", response);

    if (!buckets.empty())
    {
        writeString(R"(,"buckets":[)", response);
        bool first_bucket = true;
        for (const auto & [rule, lower, upper, bucket_count] : buckets)
        {
            if (!first_bucket)
                writeString(",", response);
            first_bucket = false;

            writeString("[", response);
            writeText(rule, response);
            writeString(",\"", response);
            writeScalar(response, lower);
            writeString("\",\"", response);
            writeScalar(response, upper);
            writeString("\",\"", response);
            writeScalar(response, bucket_count);
            writeString("\"]", response);
        }
        writeString("]", response);
    }

    writeString("}", response);
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseInstantVectorBlockWithHistograms(
    WriteBuffer & response, const Block & result_block, bool first)
{
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;

    /// The `histogram` column is Nullable(<payload tuple>); its type was validated by the caller.
    const auto & histogram_column_with_type = result_block.getByName(TimeSeriesColumnNames::Histogram);
    const auto & nullable_histogram_column = typeid_cast<const ColumnNullable &>(*histogram_column_with_type.column);
    const auto payload_columns
        = resolveHistogramPayloadColumns(removeNullable(histogram_column_with_type.type), nullable_histogram_column.getNestedColumn());

    bool need_comma = !first;
    bool emitted_any = false;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        const bool is_histogram = !nullable_histogram_column.isNullAt(i);

        /// A result row carries exactly one sample: the newest of either type per series at the evaluation time
        /// (precedence resolved in `finalizeSQL`, see StoreMethod::HISTOGRAM_GRID); both being NULL is impossible.
        if (!is_histogram && value_column->isNullAt(i))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query returned a row where both `value` and `histogram` are NULL");

        /// Defensively skip stale-marker samples.
        if (is_histogram && (payload_columns.flags->getUInt(i) & TimeSeriesHistogramFlags::StaleMarker))
            continue;

        if (need_comma)
            writeString(",", response);
        need_comma = true;
        emitted_any = true;

        writeString("{", response);

        // Write metric labels
        writeString(R"("metric":)", response);
        writeTags(response, result_block, i);

        writeString(",", response);

        // Write timestamp
        DateTime64 timestamp = timestamp_column->getInt(i);

        if (is_histogram)
        {
            // Write histogram [timestamp, {<histogram object>}]
            writeString(R"("histogram":[)", response);
            writeTimestamp(response, timestamp, timestamp_scale);
            writeString(",", response);
            writeHistogram(response, payload_columns, i);
            writeString("]}", response);
        }
        else
        {
            // Write value [timestamp, "value"]
            writeString("\"value\":[", response);
            writeTimestamp(response, timestamp, timestamp_scale);
            writeString(",\"", response);
            writeScalar(response, value_column->getFloat64(i));
            writeString("\"]}", response);
        }
    }

    return emitted_any;
}

bool PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlockWithHistograms(
    WriteBuffer & response, const Block & result_block, bool first)
{
    const auto & time_series_column = result_block.getByName(TimeSeriesColumnNames::TimeSeries).column;
    const auto & array_column = typeid_cast<const ColumnArray &>(*time_series_column);
    const auto & offsets = array_column.getOffsets();
    const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
    const auto & timestamp_column = tuple_column.getColumn(0);
    const auto & value_column = tuple_column.getColumn(1);

    auto timestamp_data_type
        = typeid_cast<const DataTypeTuple &>(
              *typeid_cast<const DataTypeArray &>(*result_block.getByName(TimeSeriesColumnNames::TimeSeries).type).getNestedType())
              .getElement(0);

    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    /// The `histogram_series` column is Array(Tuple(timestamp, <payload tuple>)); the tuple is positional.
    const auto & histogram_series_column_with_type = result_block.getByName(TimeSeriesColumnNames::HistogramSeries);
    const auto & histogram_array_column = typeid_cast<const ColumnArray &>(*histogram_series_column_with_type.column);
    const auto & histogram_offsets = histogram_array_column.getOffsets();
    const auto & histogram_tuple_column = typeid_cast<const ColumnTuple &>(histogram_array_column.getData());
    const auto & histogram_timestamp_column = histogram_tuple_column.getColumn(0);
    const auto payload_columns = resolveHistogramPayloadColumns(
        typeid_cast<const DataTypeTuple &>(
            *typeid_cast<const DataTypeArray &>(*histogram_series_column_with_type.type).getNestedType())
            .getElement(1),
        histogram_tuple_column.getColumn(1));

    bool need_comma = !first;
    bool emitted_any = false;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        size_t start = (i == 0) ? 0 : offsets[i - 1];
        size_t end = offsets[i];
        size_t histogram_start = (i == 0) ? 0 : histogram_offsets[i - 1];
        size_t histogram_end = histogram_offsets[i];

        /// Stale markers are skipped below, so a series left with nothing but them has no samples
        /// to report. Drop the whole series, as the instant-vector path does: emitting it would
        /// produce a matrix element with neither "values" nor "histograms".
        bool has_histogram_samples = false;
        for (size_t j = histogram_start; j < histogram_end && !has_histogram_samples; ++j)
            has_histogram_samples = !(payload_columns.flags->getUInt(j) & TimeSeriesHistogramFlags::StaleMarker);
        if (start == end && !has_histogram_samples)
            continue;

        if (need_comma)
            writeString(",", response);

        writeString("{", response);

        // Write labels
        writeString(R"("metric":)", response);
        writeTags(response, result_block, i);

        // Write float samples, if any
        if (start < end)
        {
            writeString(R"(,"values":[)", response);

            for (size_t j = start; j < end; ++j)
            {
                if (j > start)
                    writeString(",", response);

                writeString("[", response);
                DateTime64 timestamp = timestamp_column.getInt(j);
                writeTimestamp(response, timestamp, timestamp_scale);
                writeString(",\"", response);
                Float64 value = value_column.getFloat64(j);
                writeScalar(response, value);
                writeString("\"]", response);
            }

            writeString("]", response);
        }

        // Write histogram samples, if any
        bool wrote_histograms_key = false;

        for (size_t j = histogram_start; j < histogram_end; ++j)
        {
            /// Defensively skip stale-marker samples.
            if (payload_columns.flags->getUInt(j) & TimeSeriesHistogramFlags::StaleMarker)
                continue;

            if (!wrote_histograms_key)
            {
                writeString(R"(,"histograms":[)", response);
                wrote_histograms_key = true;
            }
            else
            {
                writeString(",", response);
            }

            writeString("[", response);
            DateTime64 timestamp = histogram_timestamp_column.getInt(j);
            writeTimestamp(response, timestamp, timestamp_scale);
            writeString(",", response);
            writeHistogram(response, payload_columns, j);
            writeString("]", response);
        }

        if (wrote_histograms_key)
            writeString("]", response);

        writeString("}", response);
        need_comma = true;
        emitted_any = true;
    }

    return emitted_any;
}


ASTPtr PrometheusHTTPProtocolAPI::makeSeriesIDsQuery(
    const Strings & match_params,
    const String & start_param,
    const String & end_param)
{
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    auto timestamp_data_type = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type).first;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    /// The optional `start` and `end` parameters are parsed the same way as on the query endpoints.
    std::optional<DateTime64> min_time;
    std::optional<DateTime64> max_time;
    if (!start_param.empty())
        min_time = parseTimeSeriesTimestamp(start_param, timestamp_scale);
    if (!end_param.empty())
        max_time = parseTimeSeriesTimestamp(end_param, timestamp_scale);
    if (min_time && max_time && (*max_time < *min_time))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'start' must not be greater than 'end'");

    /// Like the query path, filter by the [min_time, max_time] stored in the tags table; without stored bounds the range is ignored (a superset is allowed).
    auto time_series_settings = time_series_storage->getStorageSettings();
    if (!(*time_series_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
        || !(*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time])
    {
        min_time.reset();
        max_time.reset();
    }

    auto tags_table_id = time_series_storage->getTargetTableID(ViewTarget::Tags, getContext());

    /// Each `match[]` value must be an instant selector; the result is the union of the series matched by each selector.
    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->union_mode = SelectUnionMode::UNION_ALL;
    auto list_of_selects = make_intrusive<ASTExpressionList>();

    for (const auto & match_param : match_params)
    {
        PrometheusQueryTree selector;
        String error_message;
        if (!selector.tryParse(match_param, timestamp_scale, &error_message))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse the value {} of the 'match[]' parameter: {}",
                            quoteString(match_param), error_message);

        const auto * root = selector.getRoot();
        if (!root || (root->node_type != PrometheusQueryTree::NodeType::InstantSelector))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value {} of the 'match[]' parameter is not an instant selector",
                            quoteString(match_param));

        const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers;
        if (matchers.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value {} of the 'match[]' parameter must contain at least one matcher",
                            quoteString(match_param));

        auto select_ids_query = StorageTimeSeriesSelector::makeSelectIDsQuery(
            tags_table_id, matchers, *time_series_settings, min_time, max_time, timestamp_data_type);
        const auto & select_ids = typeid_cast<const ASTSelectWithUnionQuery &>(*select_ids_query);
        list_of_selects->children.push_back(select_ids.list_of_selects->children.at(0));
    }

    union_query->children.push_back(std::move(list_of_selects));
    union_query->list_of_selects = union_query->children.back();
    return union_query;
}


void PrometheusHTTPProtocolAPI::getSeries(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    /// Prometheus requires at least one `match[]` selector here; without it the endpoint would scan the whole tags table.
    if (match_params.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The Prometheus /api/v1/series endpoint requires at least one 'match[]' series selector");

    auto series_ids_query = makeSeriesIDsQuery(match_params, start_param, end_param);

    /// SELECT DISTINCT timeSeriesIdToTags(series_id) AS tags FROM (<series_ids_query>) [LIMIT <limit> + 1]
    /// timeSeriesIdToTags returns the tags registered by the inner query (including `__name__`); `DISTINCT` dedups, and one extra row detects truncation.
    auto tags_expression = makeASTFunction("timeSeriesIdToTags", make_intrusive<ASTIdentifier>("series_id"));
    tags_expression->setAlias(TimeSeriesColumnNames::Tags);

    std::optional<UInt64> sql_limit;
    if (limit)
        sql_limit = limit + 1;

    auto sql_query = makeSelectFromSubquery({std::move(tags_expression)}, std::move(series_ids_query), /* distinct = */ true, sql_limit);

    LOG_TRACE(log, "SQL query to execute:\n{}", sql_query->formatForLogging());

    /// Functions timeSeriesStoreTags() and timeSeriesIdToTags() are supported by the analyzer only.
    getContext()->setSetting("allow_experimental_analyzer", true);

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Pull the first non-empty block before writing the header so an early exception still produces the correct error response.
        bool has_output = false;
        Block block;
        while (executor.pull(block))
        {
            if (block.rows() > 0)
            {
                has_output = true;
                break;
            }
        }

        writeString(R"({"status":"success","data":[)", response);

        UInt64 written = 0;
        bool truncated = false;

        auto write_block = [&](const Block & result_block)
        {
            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                if (limit && (written == limit))
                {
                    truncated = true;
                    return;
                }
                if (written)
                    writeString(",", response);
                writeTags(response, result_block, i);
                ++written;
            }
        };

        if (has_output)
        {
            write_block(block);
            while (!truncated && executor.pull(block))
            {
                if (block.rows() > 0)
                    write_block(block);
            }
        }

        if (truncated)
            writeString(R"(],"warnings":["results truncated due to limit"]})", response);
        else
            writeString("]}", response);

        /// Finalize the query result cache write before the executor's destructor cancels the pipeline; a truncated (incomplete) result must not be cached.
        if (!truncated)
            io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early, flush the response and record QueryFinish.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getMetadata(
    WriteBuffer & response,
    const String & metric_param,
    Int64 limit,
    Int64 limit_per_metric,
    QueryFinishCallback query_finish_callback)
{
    const auto time_series_storage_id = time_series_storage->getStorageID();

    /// The Metrics target table may declare its columns as String, LowCardinality(String) or Nullable(String),
    /// so normalize them to plain strings with NULL meaning an empty string.
    auto normalize_column = [](const char * column_name)
    {
        return makeASTFunction(
            "ifNull",
            makeASTFunction("toString", make_intrusive<ASTIdentifier>(column_name)),
            make_intrusive<ASTLiteral>(String{}));
    };

    /// groupUniqArray() deduplicates the metadata entries of each metric family: the Metrics target table typically
    /// contains duplicate rows until they're merged. With `limit_per_metric` set it also caps the number of entries
    /// per family, choosing an arbitrary subset like Prometheus does. arraySort() and ORDER BY make the result deterministic.
    auto group_uniq_array = makeASTFunction(
        "groupUniqArray",
        makeASTFunction(
            "tuple",
            normalize_column(TimeSeriesColumnNames::Type),
            normalize_column(TimeSeriesColumnNames::Help),
            normalize_column(TimeSeriesColumnNames::Unit)));
    if (limit_per_metric > 0)
        group_uniq_array = addParametersToAggregateFunction(std::move(group_uniq_array), make_intrusive<ASTLiteral>(limit_per_metric));

    auto metric_family = normalize_column(TimeSeriesColumnNames::MetricFamilyName);
    metric_family->setAlias("metric_family");
    auto metadata_entries = makeASTFunction("arraySort", std::move(group_uniq_array));
    metadata_entries->setAlias("metadata");

    /// SELECT ifNull(toString(metric_family_name), '') AS metric_family, arraySort(groupUniqArray(...)) AS metadata
    /// FROM timeSeriesMetrics(database, table) [WHERE metric_family_name = metric]
    /// GROUP BY ... ORDER BY ... [LIMIT limit]
    PrometheusQueryToSQL::SelectQueryBuilder builder;
    builder.select_list.push_back(std::move(metric_family));
    builder.select_list.push_back(std::move(metadata_entries));
    builder.from_table_function = makeASTFunction(
        "timeSeriesMetrics",
        make_intrusive<ASTLiteral>(time_series_storage_id.getDatabaseName()),
        make_intrusive<ASTLiteral>(time_series_storage_id.getTableName()));

    /// Filter on the raw column so the primary key of the Metrics target table can be used.
    if (!metric_param.empty())
        builder.where = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            make_intrusive<ASTLiteral>(metric_param));

    builder.group_by.push_back(normalize_column(TimeSeriesColumnNames::MetricFamilyName));
    builder.order_by.push_back(normalize_column(TimeSeriesColumnNames::MetricFamilyName));
    builder.order_direction = 1;

    /// LIMIT 0 returns an empty result, matching how Prometheus handles `limit=0`.
    if (limit >= 0)
        builder.limit = static_cast<size_t>(limit);

    auto sql_query = builder.getSelectQuery();

    LOG_TRACE(log, "SQL query to execute:\n{}", sql_query->formatForLogging());

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Pull the first non-empty block before writing the header so an early exception still produces the correct error response.
        bool has_output = false;
        Block block;
        while (executor.pull(block))
        {
            if (block.rows() > 0)
            {
                has_output = true;
                break;
            }
        }

        writeString(R"({"status":"success","data":{)", response);

        bool first_metric_family = true;

        auto write_block = [&](const Block & result_block)
        {
            const auto & metric_family_column = *result_block.getByName("metric_family").column;
            const auto & metadata_column = typeid_cast<const ColumnArray &>(*result_block.getByName("metadata").column);
            const auto & offsets = metadata_column.getOffsets();
            const auto & entry_column = typeid_cast<const ColumnTuple &>(metadata_column.getData());
            const auto & type_column = entry_column.getColumn(0);
            const auto & help_column = entry_column.getColumn(1);
            const auto & unit_column = entry_column.getColumn(2);

            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                if (!first_metric_family)
                    writeString(",", response);
                first_metric_family = false;

                writeJSONString(metric_family_column.getDataAt(i), response, format_settings);
                writeString(":[", response);

                size_t start = (i == 0) ? 0 : offsets[i - 1];
                size_t end = offsets[i];

                for (size_t j = start; j < end; ++j)
                {
                    if (j > start)
                        writeString(",", response);

                    writeString(R"({"type":)", response);
                    writeJSONString(type_column.getDataAt(j), response, format_settings);
                    writeString(R"(,"help":)", response);
                    writeJSONString(help_column.getDataAt(j), response, format_settings);
                    writeString(R"(,"unit":)", response);
                    writeJSONString(unit_column.getDataAt(j), response, format_settings);
                    writeString("}", response);
                }

                writeString("]", response);
            }
        };

        if (has_output)
        {
            write_block(block);
            while (executor.pull(block))
            {
                if (block.rows() > 0)
                    write_block(block);
            }
        }

        writeString("}}", response);

        /// Finalize the query result cache write before the executor's destructor cancels the pipeline.
        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early, flush the response and record QueryFinish.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getLabels(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    /// Unlike /api/v1/series, the `match[]` selectors are optional here: without them the endpoint
    /// returns the label names of all the time series stored in the table.
    Strings selectors = match_params;
    if (selectors.empty())
        selectors.push_back(R"({__name__!=""})");

    auto series_ids_query = makeSeriesIDsQuery(selectors, start_param, end_param);

    /// SELECT arraySort(groupUniqArrayArray(tupleElement(timeSeriesIdToTags(series_id), 1))) AS labels FROM (<series_ids_query>)
    /// timeSeriesIdToTags returns the tags registered by the inner query (including `__name__`), so the label names
    /// are the first elements of the returned pairs; groupUniqArrayArray dedups them across all the matched series,
    /// and arraySort returns them in sorted order like Prometheus does.
    auto labels_expression = makeASTFunction(
        "arraySort",
        makeASTFunction(
            "groupUniqArrayArray",
            makeASTFunction(
                "tupleElement",
                makeASTFunction("timeSeriesIdToTags", make_intrusive<ASTIdentifier>("series_id")),
                make_intrusive<ASTLiteral>(1u))));
    labels_expression->setAlias("labels");

    auto sql_query = makeSelectFromSubquery({std::move(labels_expression)}, std::move(series_ids_query), /* distinct = */ false, {});

    LOG_TRACE(log, "SQL query to execute:\n{}", sql_query->formatForLogging());

    /// Functions timeSeriesStoreTags() and timeSeriesIdToTags() are supported by the analyzer only.
    getContext()->setSetting("allow_experimental_analyzer", true);

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Pull the first non-empty block before writing the header so an early exception still produces the correct error response.
        /// The aggregation produces exactly one row holding the array of all the label names.
        bool has_output = false;
        Block block;
        while (executor.pull(block))
        {
            if (block.rows() > 0)
            {
                has_output = true;
                break;
            }
        }

        writeString(R"({"status":"success","data":[)", response);

        UInt64 written = 0;
        bool truncated = false;

        auto write_block = [&](const Block & result_block)
        {
            const auto & array_column = typeid_cast<const ColumnArray &>(*result_block.getByName("labels").column);
            const auto & offsets = array_column.getOffsets();
            const auto & name_column = array_column.getData();

            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                size_t start = (i == 0) ? 0 : offsets[i - 1];
                for (size_t j = start; j < offsets[i]; ++j)
                {
                    if (limit && (written == limit))
                    {
                        truncated = true;
                        return;
                    }
                    if (written)
                        writeString(",", response);
                    writeJSONString(name_column.getDataAt(j), response, format_settings);
                    ++written;
                }
            }
        };

        if (has_output)
        {
            write_block(block);
            while (!truncated && executor.pull(block))
            {
                if (block.rows() > 0)
                    write_block(block);
            }
        }

        if (truncated)
            writeString(R"(],"warnings":["results truncated due to limit"]})", response);
        else
            writeString("]}", response);

        /// Finalize the query result cache write before the executor's destructor cancels the pipeline.
        /// The SQL query doesn't depend on `limit`, so its result is complete even when the response is truncated.
        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early, flush the response and record QueryFinish.
    finishExecutedQuery(io, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getLabelValues(
    WriteBuffer & response,
    const String & /* label_name */,
    const String & /* match_param */,
    const String & /* start_param */,
    const String & /* end_param */)
{
    UNUSED(response);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The label values endpoint is not implemented");
}


void PrometheusHTTPProtocolAPI::writeTags(WriteBuffer & response, const Block & result_block, size_t row_index)
{
    const auto & tags_column = result_block.getByName(TimeSeriesColumnNames::Tags).column;
    const auto & array_column = typeid_cast<const ColumnArray &>(*tags_column);
    const auto & offsets = array_column.getOffsets();
    const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
    const auto & key_column = tuple_column.getColumn(0);
    const auto & value_column = tuple_column.getColumn(1);

    writeString("{", response);

    size_t start = (row_index == 0) ? 0 : offsets[row_index - 1];
    size_t end = offsets[row_index];

    for (size_t j = start; j < end; ++j)
    {
        if (j > start)
            writeString(",", response);

        auto key = key_column.getDataAt(j);
        writeJSONString(key, response, format_settings);

        writeString(":", response);

        auto value = value_column.getDataAt(j);
        writeJSONString(value, response, format_settings);
    }

    writeString("}", response);
}


void PrometheusHTTPProtocolAPI::writeTimestamp(WriteBuffer & response, DateTime64 value, UInt32 scale)
{
    writeText(value, scale, response);
}

void PrometheusHTTPProtocolAPI::writeScalar(WriteBuffer & response, Float64 value)
{
    if (std::isfinite(value))
    {
        writeFloatText(value, response);
    }
    else if (std::isinf(value))
    {
        response.write((value > 0) ? '+' : '-');
        writeString("Inf", response);
    }
    else
    {
        writeString("NaN", response);
    }
}

}
