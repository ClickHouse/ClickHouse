#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/isValidUTF8.h>
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
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/splitTimeSeriesType.h>
#include <Storages/TimeSeries/timeSeriesMatchersToAST.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <DataTypes/IDataType.h>
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
#include <Interpreters/DatabaseCatalog.h>
#include <fmt/format.h>
#include <Common/re2.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace
{

bool containsUnsupportedPrometheusRegexpEscape(std::string_view regexp)
{
    bool escaped = false;
    for (const char character : regexp)
    {
        if (character == '\\')
        {
            escaped = !escaped;
            continue;
        }

        if (escaped && character == 'C')
            return true;
        escaped = false;
    }
    return false;
}

/// Whether a matcher matches a series that does not have the label at all, under the Prometheus
/// semantics "a missing label is equal to the empty label value". A regexp matcher is evaluated
/// against the empty string with the same fully-anchored RE2 semantics as the generated `match`
/// condition; an invalid regexp is rejected here (fail closed), like in the Prometheus parser.
bool matcherCanMatchEmptyLabelValue(const PrometheusQueryTree::Matcher & matcher, const String & match_param)
{
    switch (matcher.matcher_type)
    {
        case PrometheusQueryTree::MatcherType::EQ:
            return matcher.label_value.empty();
        case PrometheusQueryTree::MatcherType::NE:
            return !matcher.label_value.empty();
        case PrometheusQueryTree::MatcherType::RE:
        case PrometheusQueryTree::MatcherType::NRE:
        {
            if (containsUnsupportedPrometheusRegexpEscape(matcher.label_value))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid value {} of the 'match[]' parameter: regexp escape sequence \\C is not supported",
                    quoteString(match_param));

            re2::RE2 regexp(matcher.label_value, re2::RE2::Quiet);
            if (!regexp.ok())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid value {} of the 'match[]' parameter: cannot parse the regexp of the {}{}~{} matcher: {}",
                    quoteString(match_param),
                    matcher.label_name,
                    (matcher.matcher_type == PrometheusQueryTree::MatcherType::NRE) ? "!" : "=",
                    quoteString(matcher.label_value),
                    regexp.error());
            bool empty_matches_regexp = re2::RE2::FullMatch("", regexp);
            return (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE) ? empty_matches_regexp : !empty_matches_regexp;
        }
    }

    UNREACHABLE();
}

/// The `match[]` parameter of the metadata endpoints is a Prometheus series selector: a bare metric name
/// (`go_info`) or a full instant selector with label matchers (`go_info{group="PROD"}`, `{job=~"prom.*"}`).
/// Grafana emits the selector forms when it narrows label names / label values by filters. Each value is
/// parsed with the same PromQL parser as `/api/v1/query`, and each label matcher is translated into the
/// same condition over the `tags` table that the real query path builds in `StorageTimeSeriesSelector`
/// (see `timeSeriesMatcherToAST`), so the metadata endpoints filter series exactly like the query
/// endpoints do. Prometheus defines the result of a repeated `match[]` as the union of the series matched
/// by each selector, hence the disjunction over the parameters. Each value is parsed with the PromQL
/// metric-selector grammar used by the query parser. Returns an empty string when there is no filter
/// to apply (no `match[]` values at all).
///
/// An explicitly empty `match[]` value (e.g. `?match[]=`), a value that is not an instant selector (e.g.
/// a range selector `up[5m]` or a PromQL expression), the empty selector `{}`, and a selector whose
/// matchers all match the empty label value (e.g. `{job=~".*"}` - see `matcherCanMatchEmptyLabelValue`)
/// are rejected (fail closed), as in Prometheus, instead of being silently dropped or treated as an
/// unfiltered result. The last rule is what keeps the "`match[]` is required" guard on `/api/v1/series`
/// meaningful: without it `match[]={job=~".*"}` would degenerate into a full scan of the `tags` table.
String makeMatchCondition(const Strings & match_params, const std::unordered_map<String, String> & column_name_by_tag_name)
{
    ASTs selector_conditions;
    for (const auto & match_param : match_params)
    {
        if (match_param.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid empty value of the 'match[]' parameter: expected a series selector");

        PrometheusQueryTree selector;
        String error_message;
        if (!selector.tryParseMetricSelector(match_param, /* timestamp_scale_ = */ 3, &error_message))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: cannot parse a series selector: {}",
                quoteString(match_param), error_message);

        const auto * root = selector.getRoot();
        if (!root || root->node_type != PrometheusQueryTree::NodeType::InstantSelector)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: expected a series selector, not a PromQL expression",
                quoteString(match_param));

        const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers;
        if (matchers.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: the selector must contain at least one matcher",
                quoteString(match_param));

        /// Prometheus rejects a selector whose matchers all match the empty label value (i.e. all match
        /// a series that has none of the mentioned labels), because such a selector does not narrow the
        /// series set at all: `{job=~".*"}` would degenerate into a full scan of the `tags` table.
        /// Every matcher is checked (no short-circuit), so an invalid regexp anywhere in the selector
        /// is rejected here with `bad_data` instead of failing later inside the generated SQL.
        bool has_non_empty_matcher = false;
        for (const auto & matcher : matchers)
            has_non_empty_matcher |= !matcherCanMatchEmptyLabelValue(matcher, match_param);
        if (!has_non_empty_matcher)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} of the 'match[]' parameter: the selector must contain at least one matcher "
                "that does not match the empty label value",
                quoteString(match_param));

        ASTs matcher_asts;
        matcher_asts.reserve(matchers.size());
        for (const auto & matcher : matchers)
            matcher_asts.push_back(timeSeriesMatcherToAST(matcher, column_name_by_tag_name));
        selector_conditions.push_back(makeASTForLogicalAnd(std::move(matcher_asts)));
    }

    if (selector_conditions.empty())
        return {};

    /// The parenthesized form keeps the condition intact when the caller appends it to other
    /// conditions with a plain textual " AND ".
    return fmt::format("({})", makeASTForLogicalOr(std::move(selector_conditions))->formatWithSecretsOneLine());
}

/// Prometheus escapes label names that are not legacy names ([a-zA-Z_][a-zA-Z0-9_]*) using the "values"
/// escaping scheme before putting them into the /api/v1/label/<name>/values path: e.g. the tag
/// http.status_code is requested as U__http_2e_status__code. Decode it back so the endpoint looks up
/// the real stored tag key; otherwise dotted/slashed tag names are unqueryable. This mirrors Prometheus'
/// model.UnescapeName for ValueEncoding: a name is decoded only when it starts with U__; then __ is an
/// escaped underscore and _<hex>_ is an escaped code point. A malformed escape leaves the name unchanged.
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

        /// __ means a literal underscore.
        if (i + 1 < name.size() && name[i + 1] == '_')
        {
            result += '_';
            i += 2;
            continue;
        }

        /// _<hex>_ means the code point with that value. Prometheus accepts at most six hex
        /// digits here, which is enough to represent the largest Unicode code point.
        size_t closing = name.find('_', i + 1);
        if (closing == String::npos || closing == i + 1)
            return name;
        if (closing - (i + 1) > 6)
            return name;

        UInt32 code_point = 0;
        for (size_t j = i + 1; j < closing; ++j)
        {
            if (!isHexDigit(name[j]))
                return name;
            char h = name[j];
            UInt32 digit = (h >= '0' && h <= '9') ? (h - '0') : ((h | 0x20) - 'a' + 10);
            code_point = code_point * 16 + digit;
        }

        /// convertCodePointToUTF8() does not reject values outside the Unicode scalar range,
        /// including UTF-16 surrogate code points, so validate them before converting.
        if (code_point > 0x10FFFF || UTF8::isSurrogateCodePoint(code_point))
            return name;

        char utf8_bytes[4];
        size_t utf8_length = UTF8::convertCodePointToUTF8(static_cast<int>(code_point), utf8_bytes, sizeof(utf8_bytes));
        if (utf8_length == 0)
            return name;
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

namespace Setting
{
    extern const SettingsBool enable_materialized_cte;
}

namespace
{
constexpr UInt32 LOOKBACK_DELTA_SCALE = 9;

/// Clip a request range to the domain represented by the storage timestamp column. Values outside that
/// domain cannot be converted to the native type safely: for DateTime/UInt32 the conversion can wrap,
/// while DateTime64 has a signed Int64 tick domain that can overflow at high precision.
bool clipTimestampRangeToStorageType(
    std::optional<Decimal128> & start_time,
    std::optional<Decimal128> & end_time,
    const DataTypePtr & timestamp_data_type,
    UInt32 timestamp_scale,
    UInt32 request_timestamp_scale)
{
    WhichDataType which_data_type{timestamp_data_type};
    if (!(which_data_type.isDateTime64() || which_data_type.isDateTime() || which_data_type.isUInt32()))
        return true;

    Decimal128 min_storage_timestamp;
    Decimal128 max_storage_timestamp;
    if (which_data_type.isDateTime64())
    {
        min_storage_timestamp = Decimal128{static_cast<Int128>(std::numeric_limits<Int64>::min())};
        max_storage_timestamp = Decimal128{static_cast<Int128>(std::numeric_limits<Int64>::max())};
    }
    else
    {
        min_storage_timestamp = Decimal128{0};
        max_storage_timestamp = Decimal128{
            static_cast<Int128>(std::numeric_limits<UInt32>::max())
            * DecimalUtils::scaleMultiplier<Decimal128>(timestamp_scale)};
    }

    /// Compare in the same scale as the parsed request values. `request_timestamp_scale` is never
    /// lower than `timestamp_scale`, so this conversion cannot lose fractional request precision.
    const auto min_timestamp
        = DecimalUtils::convertTo<Decimal128>(request_timestamp_scale, min_storage_timestamp, timestamp_scale);
    const auto max_timestamp
        = DecimalUtils::convertTo<Decimal128>(request_timestamp_scale, max_storage_timestamp, timestamp_scale);

    /// A range entirely before or after the representable storage domain cannot overlap any series.
    if ((end_time && *end_time < min_timestamp) || (start_time && *start_time > max_timestamp))
        return false;

    /// Clamp only bounds that extend beyond the domain. The resulting native-type predicates are then
    /// equivalent for every representable timestamp and cannot wrap during AST conversion.
    if (start_time && *start_time < min_timestamp)
        *start_time = min_timestamp;
    if (end_time && *end_time > max_timestamp)
        *end_time = max_timestamp;

    return true;
}

/// Reduce a request timestamp to the storage precision while preserving an inclusive overlap predicate.
/// A lower bound on max_time needs a ceiling; an upper bound on min_time needs a floor. DecimalUtils' generic
/// scale conversion truncates towards zero, which is not a directional rounding operation for negative values.
DateTime64 rescaleTimestampBound(const Decimal128 & value, UInt32 storage_scale, UInt32 request_scale, bool round_up)
{
    if (storage_scale >= request_scale)
        return DecimalUtils::convertTo<DateTime64>(storage_scale, value, request_scale);

    const auto divisor = DecimalUtils::scaleMultiplier<Decimal128>(request_scale - storage_scale);
    auto quotient = value.value / divisor;
    const auto remainder = value.value % divisor;

    if (remainder != 0 && ((round_up && value.value > 0) || (!round_up && value.value < 0)))
        quotient += round_up ? Int128{1} : Int128{-1};

    return DateTime64{static_cast<Int64>(quotient)};
}

Decimal128 parsePrometheusRequestTimestamp(const String & value, UInt32 request_timestamp_scale)
{
    PrometheusQueryParsingUtil::RequestTimestampType timestamp;
    String error_message;
    size_t error_pos = 0;
    if (PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(
            value, request_timestamp_scale, timestamp, &error_message, &error_pos))
        return timestamp;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} at position {}", error_message, error_pos);
}

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

String makeMapTagValuesExpression(const String & tag_name)
{
    const String labels_expr = fmt::format(
        "arrayZip(mapKeys({0}), mapValues({0}))",
        TimeSeriesColumnNames::Tags);
    return fmt::format(
        "arrayMap(x -> x.2, arrayFilter(x -> (x.1 = {0} AND x.2 != ''), {1}))",
        quoteString(tag_name),
        labels_expr);
}

String makeTagCarrierConflictCondition(const String & tag_name, const String & column_name)
{
    String column_expr = fmt::format("coalesce(toString({}), '')", backQuoteIfNeed(column_name));
    String map_values_expr = makeMapTagValuesExpression(tag_name);
    return fmt::format(
        "(arrayUniq({0}) > 1) OR (({1} != '') AND (arrayUniq(arrayConcat([{1}], {0})) > 1))",
        map_values_expr,
        column_expr);
}

String makeMapTagConflictCondition(const String & tag_name)
{
    return fmt::format("arrayUniq({}) > 1", makeMapTagValuesExpression(tag_name));
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
    const UInt32 request_timestamp_scale = std::max(timestamp_scale, LOOKBACK_DELTA_SCALE);

    if (!params.lookback_delta_param.empty())
    {
        const auto lookback_delta = parsePrometheusLookbackDelta(params.lookback_delta_param, timestamp_scale);
        if (lookback_delta > 0)
            evaluation_settings.instant_selector_window = lookback_delta;
    }

    auto query_tree = std::make_shared<PrometheusQueryTree>();
    query_tree->parse(params.promql_query, timestamp_scale);
    LOG_TRACE(log, "Parsed PromQL query: {}. Result type: {}", params.promql_query, query_tree->getResultType());

    std::optional<Decimal128> request_start_time;
    std::optional<Decimal128> request_end_time;

    if (params.type == Type::Instant)
    {
        evaluation_settings.mode = PrometheusQueryEvaluationMode::QUERY;
        if (params.time_param.empty())
        {
            evaluation_settings.use_current_time = true;
        }
        else
        {
            request_start_time = parsePrometheusRequestTimestamp(params.time_param, request_timestamp_scale);
            request_end_time = request_start_time;
            evaluation_settings.step = 0;
        }
    }
    else if (params.type == Type::Range)
    {
        evaluation_settings.mode = PrometheusQueryEvaluationMode::QUERY_RANGE;
        request_start_time = parsePrometheusRequestTimestamp(params.start_param, request_timestamp_scale);
        request_end_time = parsePrometheusRequestTimestamp(params.end_param, request_timestamp_scale);
        evaluation_settings.step = parseTimeSeriesDuration(params.step_param, timestamp_scale);
    }

    if (request_start_time && request_end_time && *request_start_time > *request_end_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "start must not be greater than end");

    if (request_start_time || request_end_time)
    {
        if (!clipTimestampRangeToStorageType(
                request_start_time,
                request_end_time,
                evaluation_settings.timestamp_data_type,
                timestamp_scale,
                request_timestamp_scale))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Prometheus HTTP API timestamp is outside the storage timestamp range");

        if (request_start_time)
            evaluation_settings.start_time
                = rescaleTimestampBound(*request_start_time, timestamp_scale, request_timestamp_scale, /* round_up */ false);
        if (request_end_time)
            evaluation_settings.end_time
                = rescaleTimestampBound(*request_end_time, timestamp_scale, request_timestamp_scale, /* round_up */ false);
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
    WriteBuffer & response,
    PullingAsyncPipelineExecutor & pulling_executor,
    PrometheusQueryResultType result_type,
    UInt64 limit)
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
    WriteBuffer & response,
    const Block & result_block,
    UInt64 limit,
    UInt64 & emitted,
    bool & truncated)
{
    const auto & timestamp_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
    auto timestamp_data_type = result_block.getByName(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;

    bool need_comma = emitted > 0;

    for (size_t i = 0; i < result_block.rows(); ++i)
    {
        if (limit && emitted == limit)
        {
            truncated = true;
            return;
        }

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
        ++emitted;
        need_comma = true;
    }
}

void PrometheusHTTPProtocolAPI::writeQueryResponseRangeVectorBlock(
    WriteBuffer & response,
    const Block & result_block,
    UInt64 limit,
    UInt64 & emitted,
    bool & truncated)
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
        if (limit && emitted == limit)
        {
            truncated = true;
            return;
        }

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
        ++emitted;
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
    const auto & time_series_table_id = time_series_storage->getStorageID();

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

        /// Authorize the tags read through the configured TimeSeries table, as for the other metadata endpoints.
        auto table_expression = make_intrusive<ASTTableExpression>();
        table_expression->table_function = makeASTFunction(
            "timeSeriesTags",
            make_intrusive<ASTLiteral>(time_series_table_id.database_name),
            make_intrusive<ASTLiteral>(time_series_table_id.table_name));
        table_expression->children.push_back(table_expression->table_function);

        auto table = make_intrusive<ASTTablesInSelectQueryElement>();
        table->table_expression = table_expression;
        table->children.push_back(std::move(table_expression));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(std::move(table));
        auto & select_query = typeid_cast<ASTSelectQuery &>(*select_ids.list_of_selects->children.at(0));
        select_query.setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

        list_of_selects->children.push_back(select_ids.list_of_selects->children.at(0));
    }

    union_query->children.push_back(std::move(list_of_selects));
    union_query->list_of_selects = union_query->children.back();
    return union_query;
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

    /// `min_time`/`max_time` store the timestamp type of the `TimeSeries` table (`DateTime64(X)`, `DateTime`,
    /// `UInt32`, ...), which is not necessarily `DateTime64(3)`. Derive that type/scale from the `time_series`
    /// column the same way the PromQL query path does, and build the comparison literals with the same
    /// conversion path (`timeSeriesTimestampToAST`), so no precision is lost for higher-scale tables and the
    /// comparison type matches the column for non-`DateTime64` timestamps.
    auto time_series_metadata = time_series_storage->getInMemoryMetadataPtr(getContext(), false);
    auto timestamp_data_type
        = splitTimeSeriesType(time_series_metadata->columns.get(TimeSeriesColumnNames::TimeSeries).type).first;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);

    /// Parse both bounds in a wider request representation so validation does not depend on the native
    /// precision or range of this particular storage table. Parsing at DateTime64(0), for example, would
    /// collapse `start=1000.9&end=1000.1` to two equal values and accept an inverted range. Retry at a
    /// lower common scale when a valid large Unix timestamp cannot fit at the initially requested scale,
    /// but never reduce below the storage scale.
    UInt32 request_timestamp_scale = std::max(timestamp_scale, LOOKBACK_DELTA_SCALE);
    std::optional<Decimal128> start_time;
    std::optional<Decimal128> end_time;

    for (UInt32 candidate_timestamp_scale = request_timestamp_scale;; --candidate_timestamp_scale)
    {
        try
        {
            std::optional<Decimal128> candidate_start_time;
            std::optional<Decimal128> candidate_end_time;
            if (!start_param.empty())
                candidate_start_time = parsePrometheusRequestTimestamp(start_param, candidate_timestamp_scale);
            if (!end_param.empty())
                candidate_end_time = parsePrometheusRequestTimestamp(end_param, candidate_timestamp_scale);

            start_time = candidate_start_time;
            end_time = candidate_end_time;
            request_timestamp_scale = candidate_timestamp_scale;
            break;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::BAD_ARGUMENTS || candidate_timestamp_scale == timestamp_scale)
                throw;
        }
    }

    /// Keep the stricter ClickHouse behavior here: reject an inverted interval before bound rescaling.
    /// Otherwise, the overlap predicate below could return a series spanning both bounds, which is
    /// surprising for an interval.
    if (start_time && end_time && *start_time > *end_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "start must not be greater than end");

    /// `StorageTimeSeriesSelector::readImpl` uses the tags-table bounds only when both settings are enabled;
    /// otherwise the query path falls back to exact samples-table filtering. The metadata endpoint has no
    /// samples-table fallback, so follow Prometheus's approximate /series contract and return a superset when
    /// trusted bounds are unavailable instead of rejecting an otherwise valid request with `bad_data`.
    auto time_series_settings = time_series_storage->getStorageSettings();
    if (!(*time_series_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
        || !(*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time])
    {
        LOG_DEBUG(
            log,
            "Ignoring the Prometheus metadata time range because min_time/max_time filtering is unavailable; returning an approximate superset");
        return;
    }

    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    if (!tags_metadata->columns.has(TimeSeriesColumnNames::MinTime) || !tags_metadata->columns.has(TimeSeriesColumnNames::MaxTime))
    {
        LOG_DEBUG(
            log,
            "Ignoring the Prometheus metadata time range because the tags table has no min_time/max_time columns; returning an approximate superset");
        return;
    }

    /// Clip the validated request bounds before reducing them to storage precision. This also handles
    /// values just outside the UInt32 domain, such as `end=-0.1` or `start=4294967295.9`, which would
    /// otherwise truncate into the representable domain before the check.
    if (!clipTimestampRangeToStorageType(
            start_time, end_time, timestamp_data_type, timestamp_scale, request_timestamp_scale))
    {
        /// Keep the query path uniform while making an out-of-domain range return no metadata rows.
        conditions.emplace_back("0");
        return;
    }

    /// Convert the already validated and clipped request bounds separately at storage precision. For
    /// unsigned timestamp types this conversion is safe only after the domain check above.
    std::optional<DateTime64> native_start_time;
    std::optional<DateTime64> native_end_time;
    if (start_time)
        native_start_time = rescaleTimestampBound(*start_time, timestamp_scale, request_timestamp_scale, /* round_up */ true);
    if (end_time)
        native_end_time = rescaleTimestampBound(*end_time, timestamp_scale, request_timestamp_scale, /* round_up */ false);

    /// A series overlaps the requested range [start, end] when its `max_time >= start` and `min_time <= end`.
    /// This mirrors `StorageTimeSeriesSelector::makeWhereFilterForTagsTable`, which the real query path
    /// (`/api/v1/query`, `/api/v1/query_range`) uses on the `tags` table. There the comparisons are plain, so a
    /// `NULL` `min_time`/`max_time` makes the predicate evaluate to `NULL` and the row is dropped. An empty
    /// `time_series` row (a series with no samples) is stored with `NULL` bounds by
    /// `TimeSeriesSink::fillMinMaxTimeColumns`, and such a series never contributes to a ranged query. The
    /// metadata endpoints must fail closed the same way: keeping those rows with an `IS NULL OR ...` branch
    /// would let `/api/v1/series`, `/api/v1/labels`, and `/api/v1/label/<name>/values` report series and labels
    /// for an interval where `/api/v1/query` returns no data.
    if (native_start_time)
        conditions.push_back(fmt::format(
            "{0} >= {1}",
            TimeSeriesColumnNames::MaxTime,
            timeSeriesTimestampToAST(*native_start_time, timestamp_data_type)->formatWithSecretsOneLine()));

    if (native_end_time)
        conditions.push_back(fmt::format(
            "{0} <= {1}",
            TimeSeriesColumnNames::MinTime,
            timeSeriesTimestampToAST(*native_end_time, timestamp_data_type)->formatWithSecretsOneLine()));
}

/// Implements /api/v1/series: returns time series matching the supplied series selectors.
/// Queries the tags table and serializes each series as a JSON object with __name__ and all tag key-value pairs.
void PrometheusHTTPProtocolAPI::getSeries(
    WriteBuffer & response,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    /// Prometheus requires at least one `match[]` series selector on `/api/v1/series` (unlike
    /// `/labels` and `/label/<name>/values`, where it is optional). Without it the endpoint would run
    /// an unbounded `SELECT DISTINCT ... FROM <tags>` over the whole table and return a potentially
    /// huge response for a malformed or incomplete client call, so reject it (fail closed). An
    /// explicitly empty `match[]` value is rejected by `makeMatchCondition` below.
    if (match_params.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The Prometheus /api/v1/series endpoint requires at least one 'match[]' series selector");

    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    const auto & time_series_table_id = time_series_storage->getStorageID();

    /// Tags configured via `tags_to_columns` are stored in dedicated columns instead of the `tags` Map,
    /// but a supported external `tags` table can also carry such a tag in the residual Map (e.g. legacy
    /// rows written before the tag was moved to a dedicated column). Deduplicating by the raw table layout
    /// (`metric_name`, `tags`, the dedicated columns) would then return the same logical series twice, or
    /// emit the same label key twice from a single mixed row. So normalize each row to its logical
    /// Prometheus label set right in the query, with the same rules `timeSeriesStoreTags` applies on the
    /// write path: merge both carriers, collapse exact duplicates (`arrayDistinct`), drop empty values
    /// (an empty label value means the label is absent), retain empty-name entries until validation,
    /// and sort by label name. Conflicting carriers
    /// (same name, different values) survive as adjacent entries of the sorted array and are rejected
    /// during emission below, again as in `timeSeriesStoreTags`.
    auto tag_columns = getConfiguredTagColumns();

    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    String series_labels_alias = "__series_labels";
    while (tags_metadata->columns.has(series_labels_alias))
        series_labels_alias += "_";

    String labels_expr = fmt::format("arrayZip(mapKeys({0}), mapValues({0}))", TimeSeriesColumnNames::Tags);
    if (!tag_columns.empty())
    {
        String dedicated_entries;
        for (const auto & [tag_name, column_name] : tag_columns)
        {
            if (!dedicated_entries.empty())
                dedicated_entries += ", ";
            dedicated_entries += fmt::format("({}, coalesce(toString({}), ''))", quoteString(tag_name), backQuoteIfNeed(column_name));
        }
        labels_expr = fmt::format("arrayConcat({}, [{}])", labels_expr, dedicated_entries);
    }

    /// Keep the canonical metric name in the normalized label array as well. The serializer omits it from
    /// the per-series labels because it writes `__name__` separately, while `validate_labels` can detect a
    /// conflicting `tags['__name__']` carrier after the row has passed the selector and any SQL LIMIT.
    labels_expr = fmt::format(
        "arrayConcat({}, [({}, {})])",
        labels_expr,
        quoteString(TimeSeriesTagNames::MetricName),
        TimeSeriesColumnNames::MetricName);

    String select_columns = fmt::format(
        "{}, arraySort(arrayDistinct(arrayFilter(x -> (x.2 != '' OR x.1 = ''), {}))) AS `{}`",
        TimeSeriesColumnNames::MetricName,
        labels_expr,
        series_labels_alias);

    /// Read through the TimeSeries-owned table function so the configured TimeSeries table remains the
    /// authorization boundary. The physical tags target can be an inner implementation table without a
    /// separate SELECT grant.
    const String tags_table_expression = fmt::format(
        "timeSeriesTags({}, {})",
        quoteString(time_series_table_id.database_name),
        quoteString(time_series_table_id.table_name));

    /// Build query: SELECT DISTINCT metric_name, <normalized labels> FROM timeSeriesTags(...) [WHERE ...]
    /// The tags target is usually `AggregatingMergeTree`/`ReplacingMergeTree` and stores a row per write,
    /// so the same series can be present multiple times until parts are merged. `DISTINCT` deduplicates
    /// by series identity (metric name + normalized label set).
    String query = fmt::format("SELECT DISTINCT {} FROM {}", select_columns, tags_table_expression);

    std::vector<String> conditions;
    std::unordered_map<String, String> column_name_by_tag_name(tag_columns.begin(), tag_columns.end());
    if (String match_condition = makeMatchCondition(match_params, column_name_by_tag_name); !match_condition.empty())
        conditions.push_back(match_condition);
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    /// Fetch one extra row for a finite limit so the response can report truncation without scanning the
    /// complete matching series set.
    if (limit)
        query += fmt::format(" LIMIT {}", limit + 1);

    LOG_TRACE(log, "Prometheus series query: {}", query);

    /// The SQL query itself is limited to `limit + 1` rows only to detect truncation. Do not cache
    /// that partial query result, otherwise a later unlimited request can receive the truncated rows.
    auto query_context = getContext();
    if (limit)
    {
        query_context = Context::createCopy(getContext());
        query_context->setSetting("use_query_cache", false);
    }

    auto [ast, io] = executeQuery(query, query_context, {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);
        Block result_block;

        /// Exact duplicates were collapsed by `arrayDistinct`, so two adjacent entries with the same
        /// name in a sorted `__series_labels` array mean the row stores conflicting values for one
        /// label (e.g. in the residual `tags` map and in a dedicated `tags_to_columns` column). Reject
        /// it like `timeSeriesStoreTags` does on the write path instead of emitting an invalid series
        /// with a repeated label key. Validate the strings at this API boundary because external String
        /// columns can contain invalid UTF-8, while both Prometheus and the JSON serializer require it.
        auto is_valid_utf8 = [](const auto & value)
        {
            return UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size());
        };

        auto validate_row = [&](const auto & metric_name_column,
                                const auto & offsets,
                                const auto & key_column,
                                const auto & value_column,
                                size_t i)
        {
            if (metric_name_column->getDataAt(i).empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Found empty metric name in a row of the 'tags' table");

            if (!is_valid_utf8(metric_name_column->getDataAt(i)))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Found invalid UTF-8 in a metric name in a row of the 'tags' table");

            size_t start = (i == 0) ? 0 : offsets[i - 1];
            for (size_t j = start; j < offsets[i]; ++j)
            {
                if (key_column.getDataAt(j).empty())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found a tag with an empty name in a row of the 'tags' table");

                if (!is_valid_utf8(key_column.getDataAt(j)) || !is_valid_utf8(value_column.getDataAt(j)))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found invalid UTF-8 in a label name or value in a row of the 'tags' table");

                if (j > start && key_column.getDataAt(j) == key_column.getDataAt(j - 1))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found two tags with the same name {} but different values {} and {} in a row of the 'tags' table",
                        quoteString(key_column.getDataAt(j)),
                        quoteString(value_column.getDataAt(j - 1)),
                        quoteString(value_column.getDataAt(j)));
            }
        };

        auto validate_emittable_rows = [&](const Block & block, UInt64 already_emitted)
        {
            const auto & metric_name_column = block.getByName(TimeSeriesColumnNames::MetricName).column;
            const auto & array_column = typeid_cast<const ColumnArray &>(*block.getByName(series_labels_alias).column);
            const auto & offsets = array_column.getOffsets();
            const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
            const auto & key_column = tuple_column.getColumn(0);
            const auto & value_column = tuple_column.getColumn(1);

            for (size_t i = 0; i < block.rows(); ++i)
            {
                /// Do not validate the extra row used only to detect truncation.
                if (limit && (already_emitted == limit || i >= limit - already_emitted))
                    break;
                validate_row(metric_name_column, offsets, key_column, value_column, i);
            }
        };

        /// Materialize all rows that will be emitted before writing the success envelope. Otherwise a
        /// malformed row in a later block could be discovered after the response buffer has flushed,
        /// making the result depend on block boundaries and `http_response_buffer_size`.
        std::vector<Block> blocks_to_write;
        UInt64 emitted = 0;
        bool truncated = false;

        while (executor.pull(result_block))
        {
            if (result_block.rows() == 0)
                continue;

            size_t rows_to_write = result_block.rows();
            if (limit)
            {
                /// A row beyond `limit` only signals truncation and must not be validated or retained.
                if (emitted == limit)
                {
                    truncated = true;
                    break;
                }

                const UInt64 remaining = limit - emitted;
                if (remaining < rows_to_write)
                {
                    rows_to_write = static_cast<size_t>(remaining);
                    truncated = true;
                }
            }

            validate_emittable_rows(result_block, emitted);
            if (rows_to_write < result_block.rows())
                blocks_to_write.emplace_back(result_block.cloneWithCutColumns(0, rows_to_write));
            else
                blocks_to_write.emplace_back(std::move(result_block));
            emitted += rows_to_write;

            if (truncated)
                break;
        }

        bool first_row = true;
        auto write_block = [&](const Block & block)
        {
            const auto & metric_name_col = block.getByName(TimeSeriesColumnNames::MetricName).column;
            const auto & labels_col = block.getByName(series_labels_alias).column;

            /// `__series_labels` is an `Array(Tuple(String, String))` of (label name, label value)
            /// pairs, already normalized in the query: sorted by name, exact duplicates collapsed,
            /// empty values dropped. `ColumnArray(ColumnTuple(names, values))` — read the nested
            /// tuple once per block before serializing its rows.
            const auto & array_column = typeid_cast<const ColumnArray &>(*labels_col);
            const auto & offsets = array_column.getOffsets();
            const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
            const auto & key_column = tuple_column.getColumn(0);
            const auto & value_column = tuple_column.getColumn(1);

            for (size_t i = 0; i < block.rows(); ++i)
            {
                if (!first_row)
                    writeString(",", response);
                first_row = false;

                writeString(R"({"__name__":)", response);
                writeJSONString(metric_name_col->getDataAt(i), response, format_settings);

                size_t start = (i == 0) ? 0 : offsets[i - 1];
                size_t end = offsets[i];

                for (size_t j = start; j < end; ++j)
                {
                    if (key_column.getDataAt(j) == TimeSeriesTagNames::MetricName)
                        continue;
                    writeString(",", response);
                    writeJSONString(key_column.getDataAt(j), response, format_settings);
                    writeString(":", response);
                    writeJSONString(value_column.getDataAt(j), response, format_settings);
                }

                writeString("}", response);
            }
        };

        writeString(R"({"status":"success","data":[)", response);
        for (const auto & block : blocks_to_write)
        write_block(block);

        writeMetadataResponseFooter(response, truncated);

        /// Store the streamed result in the query result cache now (no-op if no cache writers exist in the pipeline):
        /// the executor's destructor cancels the pipeline processors, after which the pending write would be discarded.
        io.pipeline.finalizeWriteInQueryResultCache();
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
    const String & label_name_param,
    const Strings & match_params,
    const String & start_param,
    const String & end_param,
    UInt64 limit,
    QueryFinishCallback query_finish_callback)
{
    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    auto make_unique_alias = [&tags_metadata](String alias)
    {
        while (tags_metadata->columns.has(alias))
            alias += "_";
        return alias;
    };

    const String label_value_alias = make_unique_alias("__prometheus_label_value");
    const String metric_name_empty_alias = make_unique_alias("__prometheus_metric_name_empty");
    const String metric_name_invalid_utf8_alias = make_unique_alias("__prometheus_metric_name_invalid_utf8");
    const String metric_name_carrier_conflict_alias = make_unique_alias("__prometheus_metric_name_carrier_conflict");
    const String label_carrier_conflict_alias = make_unique_alias("__prometheus_label_carrier_conflict");

    const auto & time_series_table_id = time_series_storage->getStorageID();
    const String tags_table_expression = fmt::format(
        "timeSeriesTags({}, {})",
        quoteString(time_series_table_id.database_name),
        quoteString(time_series_table_id.table_name));

    /// Prometheus escapes non-legacy label names in the path. Decode them before comparing against
    /// tags_to_columns or indexing the tags Map.
    const String label_name = unescapePrometheusLabelName(label_name_param);
    if (label_name.empty()
        || !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label_name.data()), label_name.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid label name: {}", quoteString(label_name_param));

    auto tag_columns = getConfiguredTagColumns();
    const String metric_name_carrier_conflict_condition
        = makeTagCarrierConflictCondition(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);

    String query;
    String label_value_expr;
    String label_carrier_conflict_condition = "0";
    /// Collect WHERE conditions and join them, so the query stays valid regardless of which branch is taken.
    std::vector<String> conditions;

    if (label_name == "__name__")
    {
        /// Keep empty metric names in the result so a matched malformed row is rejected below instead of
        /// being silently treated as an absent __name__ label.
        label_value_expr = TimeSeriesColumnNames::MetricName;
    }
    else
    {
        /// If the label is configured in tags_to_columns, prefer its dedicated column and fall back
        /// to the tags Map for older or external rows that carry the label only there.
        String column_name;
        for (const auto & [tag_name, col_name] : tag_columns)
        {
            if (tag_name == label_name)
            {
                column_name = col_name;
                break;
            }
        }

        if (!column_name.empty())
        {
            /// A supported external tags table can still contain the configured tag only in the
            /// residual Map with the dedicated column empty or NULL, so fall back to the Map. A row
            /// with conflicting non-empty carriers is rejected instead of silently choosing one value.
            String column_expr = fmt::format("coalesce(toString({}), '')", backQuoteIfNeed(column_name));
            String map_value_expr = fmt::format("arrayElement({}, 1)", makeMapTagValuesExpression(label_name));
            label_value_expr = fmt::format(
                "if({0} != '', {0}, {1})",
                column_expr,
                map_value_expr);
            label_carrier_conflict_condition = makeTagCarrierConflictCondition(label_name, column_name);
            conditions.push_back(fmt::format("{} != ''", label_value_expr));
        }
        else
        {
            /// A key stored with an empty value is treated as an absent label.
            label_value_expr = fmt::format("arrayElement({}, 1)", makeMapTagValuesExpression(label_name));
            label_carrier_conflict_condition = makeMapTagConflictCondition(label_name);
            conditions.push_back(fmt::format("{} != ''", label_value_expr));
        }
    }

    /// Group by the value returned by this endpoint instead of carrying a metric name through the
    /// DISTINCT/sort pipeline. The validation flags are aggregated across every selected row, so
    /// fail-closed validation still sees malformed data even when multiple series share one value.
    query = fmt::format(
        "SELECT {0} AS `{1}`, max({2} = '') AS `{3}`, "
        "max(isValidUTF8({2}) = 0) AS `{4}`, max({5}) AS `{6}`, max({7}) AS `{8}` FROM {9}",
        label_value_expr,
        label_value_alias,
        TimeSeriesColumnNames::MetricName,
        metric_name_empty_alias,
        metric_name_invalid_utf8_alias,
        metric_name_carrier_conflict_condition,
        metric_name_carrier_conflict_alias,
        label_carrier_conflict_condition,
        label_carrier_conflict_alias,
        tags_table_expression);

    std::unordered_map<String, String> column_name_by_tag_name(tag_columns.begin(), tag_columns.end());
    if (String match_condition = makeMatchCondition(match_params, column_name_by_tag_name); !match_condition.empty())
        conditions.push_back(match_condition);
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    query += fmt::format(" GROUP BY `{}` ORDER BY `{}`", label_value_alias, label_value_alias);

    /// The limit is enforced while collecting values for the response. The query still reads all matching
    /// rows so the conflicting-carrier check cannot be skipped just because the response limit was reached.
    LOG_TRACE(log, "Prometheus label values query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);
        Block result_block;

        /// Materialize the values before writing the success envelope. External String columns can
        /// contain invalid UTF-8, while both Prometheus and the JSON serializer require valid UTF-8.
        std::vector<String> values_to_write;
        UInt64 emitted = 0;
        bool truncated = false;
        while (executor.pull(result_block))
        {
            if (result_block.rows() == 0)
                continue;

            const auto & value_col = result_block.getByName(label_value_alias).column;
            const auto & metric_name_empty_col = result_block.getByName(metric_name_empty_alias).column;
            const auto & metric_name_invalid_utf8_col = result_block.getByName(metric_name_invalid_utf8_alias).column;
            const auto & metric_name_carrier_conflict_col = result_block.getByName(metric_name_carrier_conflict_alias).column;
            const auto & label_carrier_conflict_col = result_block.getByName(label_carrier_conflict_alias).column;

            for (size_t i = 0; i < result_block.rows(); ++i)
            {
                if (metric_name_empty_col->getUInt(i))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found empty metric name in a row of the 'tags' table");

                if (metric_name_invalid_utf8_col->getUInt(i))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found invalid UTF-8 in a metric name in a row of the 'tags' table");

                if (metric_name_carrier_conflict_col->getUInt(i))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found two tags with the same name {} but different values in a row of the 'tags' table",
                        quoteString(TimeSeriesTagNames::MetricName));

                if (label_carrier_conflict_col->getUInt(i))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found two tags with the same name {} but different values in a row of the 'tags' table",
                        quoteString(label_name));

                auto value = value_col->getDataAt(i);
                if (value.empty())
                    continue;

                if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(value.data()), value.size()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Found invalid UTF-8 in a label value in a row of the 'tags' table");

                if (limit && emitted == limit)
                {
                    truncated = true;
                    continue;
                }

                values_to_write.emplace_back(value.data(), value.size());
                ++emitted;
            }
        }

        writeString(R"({"status":"success","data":[)", response);
        for (size_t i = 0; i < values_to_write.size(); ++i)
        {
            if (i > 0)
                writeString(",", response);
            writeJSONString(std::string_view(values_to_write[i]), response, format_settings);
        }
        writeMetadataResponseFooter(response, truncated);

        /// Store the buffered result in the query result cache now. The executor destructor cancels
        /// pipeline processors, so leaving this until destruction would discard the pending write.
        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    /// Release the query slot and record QueryFinish after the pipeline has been exhausted.
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
