#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Core/Types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Interpreters/DatabaseCatalog.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace
{

/// A Prometheus metric name is a "bare" name when it matches `[a-zA-Z_:][a-zA-Z0-9_:]*`.
bool isBareMetricName(const String & name)
{
    if (name.empty())
        return false;
    if (!(isAlphaASCII(name[0]) || name[0] == '_' || name[0] == ':'))
        return false;
    for (size_t i = 1; i < name.size(); ++i)
        if (!(isAlphaNumericASCII(name[i]) || name[i] == '_' || name[i] == ':'))
            return false;
    return true;
}

/// The `match[]` parameter of the metadata endpoints is, in general, a Prometheus series selector such
/// as `go_info{group="PROD"}` (Grafana emits selectors to narrow label names / label values). These
/// endpoints only support a bare metric name so far: they translate `match[]` into an exact
/// `metric_name = '<match>'` predicate. Treating a full selector as a literal metric name would look up
/// a metric literally named `go_info{group="PROD"}` and silently return the wrong metadata, so reject
/// anything that is not a bare metric name with a clear `NOT_IMPLEMENTED` error (fail closed) instead.
void checkMatchParamIsSupported(const String & match_param)
{
    if (!match_param.empty() && !isBareMetricName(match_param))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The Prometheus metadata endpoints currently support only a bare metric name in the 'match[]' "
            "parameter, not a full series selector with label matchers (e.g. '{{group=\"PROD\"}}'). Got: '{}'",
            match_param);
}

/// Prometheus allows the `match[]` parameter to be repeated, and the result is the union of the series
/// matched by each selector. Within the bare-metric-name subset supported so far that union is an exact
/// `metric_name IN (...)` predicate. Returns an empty string when there is no filter to apply (no
/// `match[]` values, or only empty ones). Each value is validated by `checkMatchParamIsSupported`, so a
/// full series selector anywhere in the list is rejected (fail closed) instead of being silently dropped.
String makeMetricNameCondition(const Strings & match_params)
{
    Strings metric_names;
    for (const auto & match_param : match_params)
    {
        checkMatchParamIsSupported(match_param);
        if (!match_param.empty())
            metric_names.push_back(match_param);
    }

    if (metric_names.empty())
        return {};

    if (metric_names.size() == 1)
        return fmt::format("{} = {}", TimeSeriesColumnNames::MetricName, quoteString(metric_names[0]));

    String list;
    for (const auto & metric_name : metric_names)
    {
        if (!list.empty())
            list += ", ";
        list += quoteString(metric_name);
    }
    return fmt::format("{} IN ({})", TimeSeriesColumnNames::MetricName, list);
}

/// Prometheus escapes label names that are not legacy names (`[a-zA-Z_][a-zA-Z0-9_]*`) using the "values"
/// escaping scheme before putting them into the `/api/v1/label/<name>/values` path: e.g. the tag
/// `http.status_code` is requested as `U__http_2e_status__code`. Decode it back so the endpoint looks up
/// the real stored tag key; otherwise a whole class of valid `TimeSeries` tag names (dotted, slashed, ...)
/// is unqueryable. This mirrors Prometheus' `model.UnescapeName` for `ValueEncoding`: a name is decoded
/// only when it starts with `U__`; then `__` is an escaped underscore and `_<hex>_` is an escaped code
/// point. A malformed escape leaves the name unchanged (fail-safe), matching Prometheus.
String unescapePrometheusLabelName(const String & name)
{
    static constexpr std::string_view prefix = "U__";
    if (!name.starts_with(prefix))
        return name;

    String result;
    result.reserve(name.size());
    size_t i = prefix.size();
    while (i < name.size())
    {
        char c = name[i];
        if (c != '_')
        {
            result += c;
            ++i;
            continue;
        }

        /// `__` -> a literal underscore.
        if (i + 1 < name.size() && name[i + 1] == '_')
        {
            result += '_';
            i += 2;
            continue;
        }

        /// `_<hex>_` -> the code point with that value.
        size_t closing = name.find('_', i + 1);
        if (closing == String::npos || closing == i + 1)
            return name; /// Not a well-formed escape; leave the name unchanged.

        UInt32 code_point = 0;
        for (size_t j = i + 1; j < closing; ++j)
        {
            if (!isHexDigit(name[j]))
                return name; /// Not a well-formed escape; leave the name unchanged.
            char h = name[j];
            UInt32 digit = (h >= '0' && h <= '9') ? (h - '0') : ((h | 0x20) - 'a' + 10);
            code_point = code_point * 16 + digit;
        }

        char utf8_bytes[4];
        size_t utf8_length = UTF8::convertCodePointToUTF8(static_cast<int>(code_point), utf8_bytes, sizeof(utf8_bytes));
        if (utf8_length == 0)
            return name; /// Not a valid code point; leave the name unchanged.
        result.append(utf8_bytes, utf8_length);
        i = closing + 1;
    }
    return result;
}

/// Closes the "data" array of a metadata endpoint response. When the optional `limit` parameter cut
/// the result short, the response carries the same warning Prometheus produces for a truncated
/// /api/v1/series, /api/v1/labels or /api/v1/label/<name>/values result.
void writeMetadataResponseFooter(WriteBuffer & response, bool truncated)
{
    if (truncated)
        writeString(R"(],"warnings":["results truncated due to limit"]})", response);
    else
        writeString("]}", response);
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
    UInt32 timestamp_scale = tryGetDecimalScale(*evaluation_settings.timestamp_data_type).value_or(0);

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
    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingPipelineExecutor executor(io.pipeline);

        /// Mind using the getResultType() method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
        writeQueryResponse(response, executor, converter.getResultType(), params.limit);

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
    WriteBuffer & response, PullingPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type, UInt64 limit)
{
    /// Pull until the first non-empty block is ready before writing the header
    /// because pulling_executor.pull() can throw an exception and it's better to catch it early and write
    /// the correct error header {"status":"error", ...} in PrometheusRequestHandler::QueryImpl.
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

    /// `limit` is the maximum number of returned series of a vector or matrix result. Rows past the
    /// limit only signal that the result is truncated and are not emitted; the pipeline is still
    /// pulled to completion so that it finishes normally (`finishExecutedQuery` releases the query
    /// slot, and a pending query result cache write stores the full, untruncated result).
    UInt64 emitted = 0;
    bool truncated = false;

    if (has_output)
    {
        writeQueryResponseBlock(response, result_type, block, /*first=*/ true, limit, emitted, truncated);

        while (pulling_executor.pull(block))
        {
            if (block.rows() > 0)
                writeQueryResponseBlock(response, result_type, block, /*first=*/ false, limit, emitted, truncated);
        }
    }

    writeQueryResponseFooter(response, truncated);
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

void PrometheusHTTPProtocolAPI::writeQueryResponseFooter(WriteBuffer & response, bool truncated)
{
    /// When the optional `limit` parameter cut the result short, the response carries the same
    /// warning Prometheus produces for a truncated /api/v1/query or /api/v1/query_range result.
    if (truncated)
        writeString(R"(]},"warnings":["results truncated due to limit"]})", response);
    else
        writeString("]}}", response);
}

void PrometheusHTTPProtocolAPI::writeQueryResponseBlock(
    WriteBuffer & response,
    PrometheusQueryResultType result_type,
    const Block & result_block,
    bool first,
    UInt64 limit,
    UInt64 & emitted,
    bool & truncated)
{
    LOG_TRACE(log, "Prometheus: Writing {} result ({} rows)", result_type, result_block.rows());

    switch (result_type)
    {
        /// `limit` is the maximum number of returned *series*; a scalar or string result is not
        /// a series and is never truncated.
        case PrometheusQueryTree::ResultType::SCALAR:
        {
            writeQueryResponseScalarBlock(response, result_block, first);
            return;
        }
        case PrometheusQueryTree::ResultType::STRING:
        {
            writeQueryResponseStringBlock(response, result_block, first);
            return;
        }
        case PrometheusQueryTree::ResultType::INSTANT_VECTOR:
        {
            writeQueryResponseInstantVectorBlock(response, result_block, limit, emitted, truncated);
            return;
        }
        case PrometheusQueryTree::ResultType::RANGE_VECTOR:
        {
            writeQueryResponseRangeVectorBlock(response, result_block, limit, emitted, truncated);
            return;
        }
    }
    UNREACHABLE();
}

void PrometheusHTTPProtocolAPI::writeQueryResponseScalarBlock(WriteBuffer & response, const Block & result_block, bool first)
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
}

void PrometheusHTTPProtocolAPI::writeQueryResponseStringBlock(WriteBuffer & response, const Block & result_block, bool first)
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
}

void PrometheusHTTPProtocolAPI::writeQueryResponseInstantVectorBlock(
    WriteBuffer & response, const Block & result_block, UInt64 limit, UInt64 & emitted, bool & truncated)
{
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;

    bool need_comma = emitted > 0;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        /// Each row is one series; a row past the limit only signals that the result is truncated.
        if (limit && emitted == limit)
        {
            truncated = true;
            return;
        }
        ++emitted;

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
    }
}

void PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlock(
    WriteBuffer & response, const Block & result_block, UInt64 limit, UInt64 & emitted, bool & truncated)
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

    bool need_comma = emitted > 0;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        /// Each row is one series; a row past the limit only signals that the result is truncated.
        if (limit && emitted == limit)
        {
            truncated = true;
            return;
        }
        ++emitted;

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
    }
}


std::vector<std::pair<String, String>> PrometheusHTTPProtocolAPI::getConfiguredTagColumns() const
{
    std::vector<std::pair<String, String>> result;
    auto settings = time_series_storage->getStorageSettings();
    const Map & tags_to_columns = (*settings)[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        result.emplace_back(tag_name, column_name);
    }
    return result;
}

void PrometheusHTTPProtocolAPI::appendTimeRangeConditions(
    std::vector<String> & conditions, const StoragePtr & tags_table, const String & start_param, const String & end_param)
{
    if (start_param.empty() && end_param.empty())
        return;

    /// `/api/v1/query` and `/api/v1/query_range` only use the tags-table `min_time`/`max_time` prefilter when
    /// `filter_by_min_time_and_max_time` is enabled (see `StorageTimeSeriesSelector::readImpl`, which gates the
    /// prefilter on `filter_by_min_time_and_max_time && store_min_time_and_max_time`); otherwise they scope the
    /// result by exact filtering from the samples table. The metadata endpoints query only the tags table and
    /// have no exact samples-table fallback, so the `min_time`/`max_time` overlap predicate is their only way to
    /// honor `start`/`end`. When the setting is disabled we must not apply that predicate — doing so would
    /// diverge from the real query path and contradict the operator's decision not to trust those columns — and
    /// since no exact fallback exists we reject `start`/`end` rather than silently ignoring the requested range.
    auto time_series_settings = time_series_storage->getStorageSettings();
    if (!(*time_series_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time])
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot apply the 'start'/'end' time range on the Prometheus metadata endpoints because the "
            "'filter_by_min_time_and_max_time' setting of the TimeSeries table is disabled. The metadata endpoints "
            "can only scope by the '{}'/'{}' columns of the 'tags' table, which this setting turns off; unlike "
            "'/api/v1/query' and '/api/v1/query_range', they have no exact samples-table fallback. Enable "
            "'filter_by_min_time_and_max_time' to use time range filtering, or omit 'start'/'end'",
            TimeSeriesColumnNames::MinTime,
            TimeSeriesColumnNames::MaxTime);

    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    if (!tags_metadata->columns.has(TimeSeriesColumnNames::MinTime) || !tags_metadata->columns.has(TimeSeriesColumnNames::MaxTime))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot apply the 'start'/'end' time range on the Prometheus metadata endpoints because the 'tags' table has no "
            "'{}'/'{}' columns. Enable the 'store_min_time_and_max_time' setting of the TimeSeries table to use time range filtering",
            TimeSeriesColumnNames::MinTime,
            TimeSeriesColumnNames::MaxTime);

    /// `min_time`/`max_time` store the timestamp type of the `TimeSeries` table (`DateTime64(X)`, `DateTime`,
    /// `UInt32`, ...), which is not necessarily `DateTime64(3)`. Derive that type/scale from the `time_series`
    /// column the same way the PromQL query path does, and build the comparison literals with the same
    /// conversion path (`timeSeriesTimestampToAST`), so no precision is lost for higher-scale tables and the
    /// comparison type matches the column for non-`DateTime64` timestamps.
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    auto timestamp_data_type
        = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type).first;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    /// Parse both bounds first so that an inverted range is rejected (matching the PromQL query path in
    /// `NodeEvaluationRangeGetter`) instead of silently matching long-lived series for an empty interval.
    std::optional<DateTime64> start_time;
    std::optional<DateTime64> end_time;
    if (!start_param.empty())
        start_time = parseTimeSeriesTimestamp(start_param, timestamp_scale);
    if (!end_param.empty())
        end_time = parseTimeSeriesTimestamp(end_param, timestamp_scale);

    if (start_time && end_time && *start_time > *end_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "start_time must not be greater than end_time");

    /// A series overlaps the requested range [start, end] when its `max_time >= start` and `min_time <= end`.
    /// This mirrors `StorageTimeSeriesSelector::makeWhereFilterForTagsTable`, which the real query path
    /// (`/api/v1/query`, `/api/v1/query_range`) uses on the `tags` table. There the comparisons are plain, so a
    /// `NULL` `min_time`/`max_time` makes the predicate evaluate to `NULL` and the row is dropped. An empty
    /// `time_series` row (a series with no samples) is stored with `NULL` bounds by
    /// `TimeSeriesSink::fillMinMaxTimeColumns`, and such a series never contributes to a ranged query. The
    /// metadata endpoints must fail closed the same way: keeping those rows with an `IS NULL OR ...` branch
    /// would let `/api/v1/series`, `/api/v1/labels`, and `/api/v1/label/<name>/values` report series and labels
    /// for an interval where `/api/v1/query` returns no data.
    if (start_time)
        conditions.push_back(fmt::format(
            "{0} >= {1}",
            TimeSeriesColumnNames::MaxTime,
            timeSeriesTimestampToAST(*start_time, timestamp_data_type)->formatWithSecretsOneLine()));

    if (end_time)
        conditions.push_back(fmt::format(
            "{0} <= {1}",
            TimeSeriesColumnNames::MinTime,
            timeSeriesTimestampToAST(*end_time, timestamp_data_type)->formatWithSecretsOneLine()));
}

/// Implements /api/v1/series: returns time series matching a metric name filter.
/// Queries the tags table and serializes each series as a JSON object with __name__ and all tag key-value pairs.
void PrometheusHTTPProtocolAPI::getSeries(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    /// Tags configured via `tags_to_columns` are stored in dedicated columns instead of the `tags` Map,
    /// so they must be selected and emitted separately. They are read back as strings and the "absent"
    /// default is rendered as an empty string.
    auto tag_columns = getConfiguredTagColumns();

    String select_columns = fmt::format("{}, {}", TimeSeriesColumnNames::MetricName, TimeSeriesColumnNames::Tags);
    for (size_t i = 0; i < tag_columns.size(); ++i)
        select_columns += fmt::format(", coalesce(toString({}), '') AS `__tsc_{}`", backQuoteIfNeed(tag_columns[i].second), i);

    /// Build query: SELECT DISTINCT metric_name, tags[, <tags_to_columns>] FROM <tags_table> [WHERE ...]
    /// The tags target is usually `AggregatingMergeTree`/`ReplacingMergeTree` and stores a row per write,
    /// so the same series can be present multiple times until parts are merged. `DISTINCT` deduplicates
    /// by series identity (metric name + full label set).
    String query = fmt::format("SELECT DISTINCT {} FROM {}", select_columns, tags_table_id.getFullTableName());

    std::vector<String> conditions;
    /// Only bare metric names are supported so far (a full series selector is rejected inside), so the
    /// `match[]` parameters map directly to an exact metric name predicate (the union of the names).
    if (String metric_name_condition = makeMetricNameCondition(match_params); !metric_name_condition.empty())
        conditions.push_back(metric_name_condition);
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    /// Each result row is emitted as exactly one series, so `LIMIT limit + 1` bounds the scan while still
    /// letting the emission loop below see whether the result was truncated (the one extra row).
    if (limit)
        query += fmt::format(" LIMIT {}", limit + 1);

    LOG_TRACE(log, "Prometheus series query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block result_block;

        /// Pull until the first non-empty block is ready before writing the success envelope: `pull` can
        /// throw after `executeQuery` has already succeeded (read error, limit exceeded, killed query, ...),
        /// and once `{"status":"success",...}` has been written `PrometheusRequestHandler::QueryImpl` can no
        /// longer replace the response with `{"status":"error",...}` (`response.sent()` is already true), so
        /// the client would get a truncated success body. Mirrors `writeQueryResponse`.
        auto pull_next_nonempty = [&]
        {
            while (executor.pull(result_block))
            {
                if (result_block.rows() > 0)
                    return true;
            }
            return false;
        };
        bool has_output = pull_next_nonempty();

        writeString(R"({"status":"success","data":[)", response);

        bool first_row = true;
        UInt64 emitted = 0;
        bool truncated = false;
        while (has_output)
        {
            const auto & metric_name_col = result_block.getByName(TimeSeriesColumnNames::MetricName).column;
            const auto & tags_col = result_block.getByName(TimeSeriesColumnNames::Tags).column;

            std::vector<const IColumn *> tag_value_cols;
            tag_value_cols.reserve(tag_columns.size());
            for (size_t c = 0; c < tag_columns.size(); ++c)
                tag_value_cols.push_back(result_block.getByName(fmt::format("__tsc_{}", c)).column.get());

            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                /// The SQL `LIMIT limit + 1` above bounds the scan, so at most one extra row shows up
                /// here; it only signals that the result is truncated and is not emitted itself.
                if (limit && emitted == limit)
                {
                    truncated = true;
                    break;
                }
                ++emitted;

                if (!first_row)
                    writeString(",", response);
                first_row = false;

                writeString(R"({"__name__":)", response);
                writeJSONString(metric_name_col->getDataAt(i), response, format_settings);

                /// The `tags` column is a `Map(String, String)`, which materializes as `ColumnMap`.
                /// `ColumnMap` wraps a `ColumnArray(ColumnTuple(keys, values))`, so read the nested array
                /// to enumerate the key-value pairs of each row.
                const auto & map_column = typeid_cast<const ColumnMap &>(*tags_col);
                const auto & array_column = map_column.getNestedColumn();
                const auto & offsets = array_column.getOffsets();
                size_t start = (i == 0) ? 0 : offsets[i - 1];
                size_t end = offsets[i];

                const auto & tuple_column = map_column.getNestedData();
                const auto & key_column = tuple_column.getColumn(0);
                const auto & value_column = tuple_column.getColumn(1);

                for (size_t j = start; j < end; ++j)
                {
                    writeString(",", response);
                    writeJSONString(key_column.getDataAt(j), response, format_settings);
                    writeString(":", response);
                    writeJSONString(value_column.getDataAt(j), response, format_settings);
                }

                /// Emit tags that were moved out of the `tags` Map into dedicated columns. An empty value
                /// means the tag is not set for this series (Prometheus treats it as absent), so skip it.
                for (size_t c = 0; c < tag_columns.size(); ++c)
                {
                    auto value = tag_value_cols[c]->getDataAt(i);
                    if (value.empty())
                        continue;
                    writeString(",", response);
                    writeJSONString(std::string_view{tag_columns[c].first}, response, format_settings);
                    writeString(":", response);
                    writeJSONString(value, response, format_settings);
                }

                writeString("}", response);
            }

            has_output = pull_next_nonempty();
        }

        writeMetadataResponseFooter(response, truncated);
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early and record QueryFinish in system.query_log, mirroring executePromQLQuery.
    /// BlockIO::~BlockIO only resets the pipeline and never runs the finish/exception callbacks, so without
    /// this a successful metadata request would never emit a QueryFinish entry and would keep the query slot
    /// occupied until the whole HTTP response is written instead of releasing it when the pipeline is exhausted.
    finishExecutedQuery(io, query_finish_callback);
}

/// Implements /api/v1/labels: returns all distinct label names across all time series.
/// Always includes "__name__" as a virtual label, then queries distinct keys from the tags Map column.
void PrometheusHTTPProtocolAPI::getLabels(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    /// Labels live in two places: keys of the `tags` Map column and, for tags configured via
    /// `tags_to_columns`, dedicated columns. A configured tag is reported only when at least one series
    /// has a non-empty value for it.
    auto tag_columns = getConfiguredTagColumns();

    String label_keys_expr = fmt::format("mapKeys({})", TimeSeriesColumnNames::Tags);
    if (!tag_columns.empty())
    {
        String configured;
        for (size_t i = 0; i < tag_columns.size(); ++i)
        {
            if (i != 0)
                configured += ", ";
            configured += fmt::format(
                "if(coalesce(toString({}), '') != '', {}, '')",
                backQuoteIfNeed(tag_columns[i].second),
                quoteString(tag_columns[i].first));
        }
        label_keys_expr = fmt::format(
            "arrayConcat(arrayMap(k -> toString(k), mapKeys({})), arrayFilter(x -> x != '', [{}]))",
            TimeSeriesColumnNames::Tags,
            configured);
    }

    /// Query distinct label keys. __name__ is always included as a virtual label.
    String query = fmt::format(
        "SELECT DISTINCT arrayJoin({}) AS label_key FROM {}", label_keys_expr, tags_table_id.getFullTableName());

    std::vector<String> conditions;
    if (String metric_name_condition = makeMetricNameCondition(match_params); !metric_name_condition.empty())
        conditions.push_back(metric_name_condition);
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    query += " ORDER BY label_key";

    /// Each result row is emitted as one label name, except the row for `__name__` which is skipped
    /// (it is always prepended as a virtual label and occurs at most once thanks to `DISTINCT`), so
    /// `LIMIT limit + 1` gives the emission loop below enough rows to fill the limit and to see
    /// whether the result was truncated.
    if (limit)
        query += fmt::format(" LIMIT {}", limit + 1);

    LOG_TRACE(log, "Prometheus labels query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block result_block;

        /// Pull until the first non-empty block is ready before writing the success envelope: `pull` can
        /// throw after `executeQuery` has already succeeded (read error, limit exceeded, killed query, ...),
        /// and once `{"status":"success",...}` has been written `PrometheusRequestHandler::QueryImpl` can no
        /// longer replace the response with `{"status":"error",...}` (`response.sent()` is already true), so
        /// the client would get a truncated success body. Mirrors `writeQueryResponse`.
        auto pull_next_nonempty = [&]
        {
            while (executor.pull(result_block))
            {
                if (result_block.rows() > 0)
                    return true;
            }
            return false;
        };
        bool has_output = pull_next_nonempty();

        writeString(R"({"status":"success","data":["__name__")", response);

        /// The virtual `__name__` label emitted above is the first item and counts towards the limit.
        UInt64 emitted = 1;
        bool truncated = false;
        while (has_output)
        {
            const auto & label_col = result_block.getByName("label_key").column;

            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                auto label = label_col->getDataAt(i);
                /// Skip __name__ since we already included it
                if (label == "__name__")
                    continue;
                if (limit && emitted == limit)
                {
                    truncated = true;
                    break;
                }
                ++emitted;
                writeString(",", response);
                writeJSONString(label, response, format_settings);
            }

            has_output = pull_next_nonempty();
        }

        writeMetadataResponseFooter(response, truncated);
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early and record QueryFinish in system.query_log, mirroring executePromQLQuery.
    /// BlockIO::~BlockIO only resets the pipeline and never runs the finish/exception callbacks, so without
    /// this a successful metadata request would never emit a QueryFinish entry and would keep the query slot
    /// occupied until the whole HTTP response is written instead of releasing it when the pipeline is exhausted.
    finishExecutedQuery(io, query_finish_callback);
}

/// Implements /api/v1/label/<name>/values: returns all distinct values for a given label name.
/// For "__name__", queries the metric_name column directly; for tags moved into dedicated columns by
/// `tags_to_columns`, reads that column; otherwise extracts values from the tags Map.
void PrometheusHTTPProtocolAPI::getLabelValues(
    WriteBuffer & response,
    const String & label_name_param,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    /// Prometheus escapes non-legacy label names in the `/api/v1/label/<name>/values` path (e.g. a tag
    /// `http.status_code` arrives as `U__http_2e_status__code`). Decode it back before comparing against
    /// `tags_to_columns` or indexing the `tags` map, otherwise dotted/slashed tag names are unqueryable.
    const String label_name = unescapePrometheusLabelName(label_name_param);

    String query;
    /// Collect WHERE conditions and join them, so the query stays valid regardless of which branch is taken.
    std::vector<String> conditions;

    if (label_name == "__name__")
    {
        /// __name__ maps to the metric_name column directly. An empty metric name is treated as an
        /// absent label (the emission loop below skips empty values anyway), so filter it out in SQL
        /// to keep result rows one-to-one with emitted items - the `LIMIT limit + 1` bound relies on it.
        query = fmt::format(
            "SELECT DISTINCT {} AS label_value FROM {}",
            TimeSeriesColumnNames::MetricName,
            tags_table_id.getFullTableName());
        conditions.push_back(fmt::format("{} != ''", TimeSeriesColumnNames::MetricName));
    }
    else
    {
        /// If the label was moved into a dedicated column via `tags_to_columns`, read it from there;
        /// otherwise it lives in the `tags` Map.
        String column_name;
        for (const auto & [tag_name, col_name] : getConfiguredTagColumns())
        {
            if (tag_name == label_name)
            {
                column_name = col_name;
                break;
            }
        }

        if (!column_name.empty())
        {
            String value_expr = fmt::format("coalesce(toString({}), '')", backQuoteIfNeed(column_name));
            query = fmt::format("SELECT DISTINCT {} AS label_value FROM {}", value_expr, tags_table_id.getFullTableName());
            conditions.push_back(fmt::format("{} != ''", value_expr));
        }
        else
        {
            /// Extract distinct values for a specific key from the tags Map. A key stored with an empty
            /// value is treated as an absent label (the emission loop below skips empty values anyway),
            /// so require a non-empty value in SQL - which also implies the key is present - to keep
            /// result rows one-to-one with emitted items; the `LIMIT limit + 1` bound relies on it.
            query = fmt::format(
                "SELECT DISTINCT {}[{}] AS label_value FROM {}",
                TimeSeriesColumnNames::Tags,
                quoteString(label_name),
                tags_table_id.getFullTableName());
            conditions.push_back(fmt::format("{}[{}] != ''",
                TimeSeriesColumnNames::Tags,
                quoteString(label_name)));
        }
    }

    if (String metric_name_condition = makeMetricNameCondition(match_params); !metric_name_condition.empty())
        conditions.push_back(metric_name_condition);
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    query += " ORDER BY label_value";

    /// Thanks to the non-empty-value conditions above each result row is emitted as exactly one label
    /// value, so `LIMIT limit + 1` bounds the scan while still letting the emission loop below see
    /// whether the result was truncated (the one extra row).
    if (limit)
        query += fmt::format(" LIMIT {}", limit + 1);

    LOG_TRACE(log, "Prometheus label values query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingPipelineExecutor executor(io.pipeline);
        Block result_block;

        /// Pull until the first non-empty block is ready before writing the success envelope: `pull` can
        /// throw after `executeQuery` has already succeeded (read error, limit exceeded, killed query, ...),
        /// and once `{"status":"success",...}` has been written `PrometheusRequestHandler::QueryImpl` can no
        /// longer replace the response with `{"status":"error",...}` (`response.sent()` is already true), so
        /// the client would get a truncated success body. Mirrors `writeQueryResponse`.
        auto pull_next_nonempty = [&]
        {
            while (executor.pull(result_block))
            {
                if (result_block.rows() > 0)
                    return true;
            }
            return false;
        };
        bool has_output = pull_next_nonempty();

        writeString(R"({"status":"success","data":[)", response);

        bool first = true;
        UInt64 emitted = 0;
        bool truncated = false;
        while (has_output)
        {
            const auto & value_col = result_block.getByName("label_value").column;

            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                auto value = value_col->getDataAt(i);
                if (value.empty())
                    continue;
                if (limit && emitted == limit)
                {
                    truncated = true;
                    break;
                }
                ++emitted;
                if (!first)
                    writeString(",", response);
                first = false;
                writeJSONString(value, response, format_settings);
            }

            has_output = pull_next_nonempty();
        }

        writeMetadataResponseFooter(response, truncated);
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot early and record QueryFinish in system.query_log, mirroring executePromQLQuery.
    /// BlockIO::~BlockIO only resets the pipeline and never runs the finish/exception callbacks, so without
    /// this a successful metadata request would never emit a QueryFinish entry and would keep the query slot
    /// occupied until the whole HTTP response is written instead of releasing it when the pipeline is exhausted.
    finishExecutedQuery(io, query_finish_callback);
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


void PrometheusHTTPProtocolAPI::writeSeriesResponse(WriteBuffer & response, const Block & /* result_block */)
{
    writeString(R"({"status":"success","data":[]})", response);
}

void PrometheusHTTPProtocolAPI::writeLabelsResponse(WriteBuffer & response, const Block & /* result_block */)
{
    writeString(R"({"status":"success","data":["__name__","job","instance"]})", response);
}

void PrometheusHTTPProtocolAPI::writeLabelValuesResponse(WriteBuffer & response, const Block & /* result_block */)
{
    writeString(R"({"status":"success","data":[]})", response);
}
}
