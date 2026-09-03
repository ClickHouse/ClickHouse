#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <base/hex.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/isValidUTF8.h>
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
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Core/Types.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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

/// Decodes a label name from the /api/v1/label/<name>/values URL path. Prometheus escapes label names
/// that are not legacy names ([a-zA-Z_][a-zA-Z0-9_]*) with the "values" scheme before putting them into
/// the path: a "U__" prefix, then "__" means a literal underscore and "_<hex>_" means the code point
/// with that value (e.g. the label "http.status" is requested as "U__http_2e_status").
/// Like UnescapeName in Prometheus, a name without the "U__" prefix or with a malformed escape sequence
/// is returned unchanged.
String unescapePrometheusLabelName(const String & name)
{
    static constexpr std::string_view prefix = "U__";
    if (!name.starts_with(prefix))
        return name;

    String result;
    result.reserve(name.size());

    size_t pos = prefix.size();
    while (pos < name.size())
    {
        if (name[pos] != '_')
        {
            result += name[pos++];
            continue;
        }

        /// "__" means a literal underscore.
        if (pos + 1 < name.size() && name[pos + 1] == '_')
        {
            result += '_';
            pos += 2;
            continue;
        }

        /// "_<hex>_" means the code point with that value. Prometheus accepts at most six hex digits here,
        /// enough to represent the largest Unicode code point.
        size_t closing = name.find('_', pos + 1);
        if (closing == String::npos || closing == pos + 1 || closing - (pos + 1) > 6)
            return name;

        UInt32 code_point = 0;
        for (size_t i = pos + 1; i < closing; ++i)
        {
            if (!isHexDigit(name[i]))
                return name;
            code_point = code_point * 16 + unhex(name[i]);
        }

        /// convertCodePointToUTF8 doesn't reject values outside the Unicode scalar range,
        /// including UTF-16 surrogate code points, so validate them beforehand.
        if (code_point > 0x10FFFF || UTF8::isSurrogateCodePoint(code_point))
            return name;

        char utf8_bytes[4];
        size_t utf8_length = UTF8::convertCodePointToUTF8(static_cast<int>(code_point), utf8_bytes, sizeof(utf8_bytes));
        result.append(utf8_bytes, utf8_length);
        pos = closing + 1;
    }
    return result;
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

    /// The generated SQL relies on `AS MATERIALIZED` to avoid evaluating subqueries referenced more than once
    /// repeatedly (see SQLSubqueryType::MATERIALIZED_TABLE), and that mark has effect only with the setting
    /// `enable_materialized_cte` enabled. Enable it unless the user set it explicitly.
    if (!getContext()->getSettingsRef()[Setting::enable_materialized_cte].changed)
        getContext()->setSetting("enable_materialized_cte", true);

    /// `AS MATERIALIZED` is honored by the analyzer only, so the generated SQL always runs the analyzer.
    getContext()->setSetting("allow_experimental_analyzer", true);

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);

        /// Mind using the getResultType() method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
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

    if (has_output)
    {
        writeQueryResponseBlock(response, result_type, block, /*first=*/ true);

        while (pulling_executor.pull(block))
        {
            if (block.rows() > 0)
                writeQueryResponseBlock(response, result_type, block, /*first=*/ false);
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

void PrometheusHTTPProtocolAPI::writeQueryResponseBlock(WriteBuffer & response, PrometheusQueryResultType result_type, const Block & result_block, bool first)
{
    LOG_TRACE(log, "Prometheus: Writing {} result ({} rows)", result_type, result_block.rows());

    switch (result_type)
    {
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
            writeQueryResponseInstantVectorBlock(response, result_block, first);
            return;
        }
        case PrometheusQueryTree::ResultType::RANGE_VECTOR:
        {
            writeQueryResponseRangeVectorBlock(response, result_block, first);
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

void PrometheusHTTPProtocolAPI::writeQueryResponseInstantVectorBlock(WriteBuffer & response, const Block & result_block, bool first)
{
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;

    bool need_comma = !first;

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
    }
}

void PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlock(WriteBuffer & response, const Block & result_block, bool first)
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

    bool need_comma = !first;

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
    }
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

    getLabelsOrLabelValues(response, std::move(labels_expression), match_params, start_param, end_param, limit, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getLabelValues(
    WriteBuffer & response,
    const String & label_name_param,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    /// Prometheus escapes label names that are not legacy names in the URL path,
    /// so decode the parameter before comparing it with the stored tag names.
    String label_name = unescapePrometheusLabelName(label_name_param);
    if (label_name.empty() || !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label_name.data()), label_name.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid label name {}", quoteString(label_name_param));

    /// SELECT arraySort(groupUniqArrayArray(arrayMap(tag -> tag.2, arrayFilter(tag -> tag.1 = <label_name> AND tag.2 != '', timeSeriesIdToTags(series_id))))) AS labels
    /// FROM (<series_ids_query>)
    /// timeSeriesIdToTags returns the (name, value) pairs of the tags registered by the inner query (including `__name__`),
    /// so the values of the requested label are the second elements of the pairs whose first element is the label name.
    /// An empty value means an absent label in Prometheus, so it's never returned.
    auto tag_element = [](UInt32 index)
    {
        return makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("tag"), make_intrusive<ASTLiteral>(index));
    };

    auto filtered_tags = makeASTFunction(
        "arrayFilter",
        makeASTLambda(
            {"tag"},
            makeASTFunction(
                "and",
                makeASTFunction("equals", tag_element(1), make_intrusive<ASTLiteral>(label_name)),
                makeASTFunction("notEquals", tag_element(2), make_intrusive<ASTLiteral>(String{})))),
        makeASTFunction("timeSeriesIdToTags", make_intrusive<ASTIdentifier>("series_id")));

    auto values_expression = makeASTFunction(
        "arraySort",
        makeASTFunction(
            "groupUniqArrayArray",
            makeASTFunction("arrayMap", makeASTLambda({"tag"}, tag_element(2)), std::move(filtered_tags))));

    getLabelsOrLabelValues(response, std::move(values_expression), match_params, start_param, end_param, limit, query_finish_callback);
}

void PrometheusHTTPProtocolAPI::getLabelsOrLabelValues(
    WriteBuffer & response,
    ASTPtr array_expression,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    /// Unlike /api/v1/series, the `match[]` selectors are optional here: without them the endpoint
    /// returns the label names (or the label values) of all the time series stored in the table.
    Strings selectors = match_params;
    if (selectors.empty())
        selectors.push_back(R"({__name__!=""})");

    auto series_ids_query = makeSeriesIDsQuery(selectors, start_param, end_param);

    array_expression->setAlias("labels");

    auto sql_query = makeSelectFromSubquery({std::move(array_expression)}, std::move(series_ids_query), /* distinct = */ false, {});

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
