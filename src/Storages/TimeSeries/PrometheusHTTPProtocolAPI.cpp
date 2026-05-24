#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
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
#include <Common/typeid_cast.h>
#include <fmt/format.h>

#include <algorithm>
#include <optional>
#include <set>
#include <vector>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsUInt64 prometheus_max_series;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_PROMQL_QUERY;
}

namespace
{

using MatcherList = PrometheusQueryTree::MatcherList;
using Matcher = PrometheusQueryTree::Matcher;
using MatcherType = PrometheusQueryTree::MatcherType;

std::optional<String> findColumnForTag(const Map & tags_to_columns, const String & label_name)
{
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        if (tag_name == label_name)
            return tuple.at(1).safeGet<String>();
    }
    return std::nullopt;
}

MatcherList parseMatchers(const String & match_param)
{
    if (match_param.empty())
        return {};

    PrometheusQueryTree tree;
    String err;
    size_t err_pos = 0;
    if (!tree.tryParse(match_param, 3, &err, &err_pos))
        throw Exception(
            ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
            "{} at position {} while parsing match[] selector: {}",
            err,
            err_pos,
            match_param);

    const auto * root = tree.getRoot();
    if (!root || root->node_type != PrometheusQueryTree::NodeType::InstantSelector)
        throw Exception(
            ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
            "match[] must be a metric name or label selector {{...}}, got a non-selector expression: {}",
            match_param);

    const auto * sel = typeid_cast<const PrometheusQueryTree::InstantSelector *>(root);
    chassert(sel);
    return sel->matchers;
}

String predicateForMatcher(const Matcher & matcher, const Map & tags_to_columns)
{
    const auto * metric_name_col = TimeSeriesColumnNames::MetricName;
    const auto * tags_col_name = TimeSeriesColumnNames::Tags;

    if (matcher.label_name == "__name__")
    {
        switch (matcher.matcher_type)
        {
            case MatcherType::EQ:
                return fmt::format("{} = {}", metric_name_col, quoteString(matcher.label_value));
            case MatcherType::NE:
                return fmt::format("{} != {}", metric_name_col, quoteString(matcher.label_value));
            case MatcherType::RE:
                return fmt::format("match({}, {})", metric_name_col, quoteString(matcher.label_value));
            case MatcherType::NRE:
                return fmt::format("NOT match({}, {})", metric_name_col, quoteString(matcher.label_value));
        }
    }

    if (auto promoted_col = findColumnForTag(tags_to_columns, matcher.label_name))
    {
        const auto col = backQuoteIfNeed(*promoted_col);
        switch (matcher.matcher_type)
        {
            case MatcherType::EQ:
                return fmt::format("{} = {}", col, quoteString(matcher.label_value));
            case MatcherType::NE:
                return fmt::format("{} != {}", col, quoteString(matcher.label_value));
            case MatcherType::RE:
                return fmt::format("match({}, {})", col, quoteString(matcher.label_value));
            case MatcherType::NRE:
                return fmt::format("NOT match({}, {})", col, quoteString(matcher.label_value));
        }
    }

    const auto key_lit = quoteString(matcher.label_name);
    const auto map_access = fmt::format("{}[{}]", tags_col_name, key_lit);
    /// Prometheus treats a missing label as an empty string (same as NE/NRE branches below).
    switch (matcher.matcher_type)
    {
        case MatcherType::EQ:
            if (matcher.label_value.empty())
                return fmt::format(
                    "(NOT mapContains({}, {})) OR (mapContains({}, {}) AND {} = {})",
                    tags_col_name,
                    key_lit,
                    tags_col_name,
                    key_lit,
                    map_access,
                    quoteString(matcher.label_value));
            return fmt::format(
                "mapContains({}, {}) AND {} = {}", tags_col_name, key_lit, map_access, quoteString(matcher.label_value));
        case MatcherType::NE:
            /// `{key!=""}` must exclude series without the label: Prometheus treats an absent label as the
            /// empty string, so absent should NOT satisfy `!= ""`. Require key presence in that case.
            if (matcher.label_value.empty())
                return fmt::format("mapContains({}, {}) AND {} != {}", tags_col_name, key_lit, map_access, quoteString(""));
            return fmt::format(
                "(NOT mapContains({}, {})) OR ({} != {})", tags_col_name, key_lit, map_access, quoteString(matcher.label_value));
        case MatcherType::RE:
            return fmt::format(
                "((NOT mapContains({}, {})) AND match({}, {})) OR (mapContains({}, {}) AND match({}, {}))",
                tags_col_name,
                key_lit,
                quoteString(""),
                quoteString(matcher.label_value),
                tags_col_name,
                key_lit,
                map_access,
                quoteString(matcher.label_value));
        case MatcherType::NRE:
            return fmt::format(
                "((NOT mapContains({}, {})) AND NOT match({}, {})) OR (mapContains({}, {}) AND NOT match({}, {}))",
                tags_col_name,
                key_lit,
                quoteString(""),
                quoteString(matcher.label_value),
                tags_col_name,
                key_lit,
                map_access,
                quoteString(matcher.label_value));
    }
    UNREACHABLE();
}

String buildTimeOverlapWhere(const TimeSeriesSettings & settings, const String & start_param, const String & end_param)
{
    if (!settings[TimeSeriesSetting::store_min_time_and_max_time] || !settings[TimeSeriesSetting::filter_by_min_time_and_max_time])
        return {};

    if (start_param.empty() && end_param.empty())
        return {};

    constexpr UInt32 ts_scale = 3;
    String predicates;
    if (!end_param.empty())
    {
        const DateTime64 end_ts = parseTimeSeriesTimestamp(end_param, ts_scale);
        predicates = fmt::format(
            "{} <= toDateTime64({}, {})",
            TimeSeriesColumnNames::MinTime,
            end_ts.value,
            ts_scale);
    }
    if (!start_param.empty())
    {
        const DateTime64 start_ts = parseTimeSeriesTimestamp(start_param, ts_scale);
        String p = fmt::format(
            "{} >= toDateTime64({}, {})",
            TimeSeriesColumnNames::MaxTime,
            start_ts.value,
            ts_scale);
        if (!predicates.empty())
            predicates += " AND ";
        predicates += p;
    }
    return predicates;
}

/// Predicate for matchers inside one PromQL instant selector (AND). Empty list matches all (`{}`).
String predicateForMatcherList(const MatcherList & matchers, const Map & tags_to_columns)
{
    if (matchers.empty())
        return "1";

    std::vector<String> parts;
    for (const auto & matcher : matchers)
        parts.push_back(predicateForMatcher(matcher, tags_to_columns));
    String out;
    for (size_t i = 0; i < parts.size(); ++i)
    {
        if (i)
            out += " AND ";
        out += parts[i];
    }
    return out;
}

/// Repeated `match[]` query params are OR across selectors (Prometheus union semantics).
String buildMatchWhere(const std::vector<String> & match_params, const Map & tags_to_columns, const String & time_predicate)
{
    std::vector<String> selector_predicates;
    for (const auto & mp : match_params)
    {
        if (mp.empty())
            continue;
        MatcherList ml = parseMatchers(mp);
        String p = predicateForMatcherList(ml, tags_to_columns);
        chassert(!p.empty());
        selector_predicates.push_back("(" + p + ")");
    }

    std::vector<String> top_parts;
    if (!selector_predicates.empty())
    {
        if (selector_predicates.size() == 1)
            top_parts.push_back(selector_predicates.front());
        else
        {
            String or_chain;
            for (size_t i = 0; i < selector_predicates.size(); ++i)
            {
                if (i)
                    or_chain += " OR ";
                or_chain += selector_predicates[i];
            }
            /// AND binds tighter than OR; wrap the union so we get `(match1 OR match2) AND time`, not `match1 OR (match2 AND time)`.
            top_parts.push_back("(" + or_chain + ")");
        }
    }
    if (!time_predicate.empty())
        top_parts.push_back(time_predicate);

    if (top_parts.empty())
        return {};

    String out = " WHERE ";
    for (size_t i = 0; i < top_parts.size(); ++i)
    {
        if (i)
            out += " AND ";
        out += top_parts[i];
    }
    return out;
}

void writeJSONPairsFromTagsColumn(
    const ColumnPtr & tags_column,
    size_t row_index,
    WriteBuffer & response,
    const FormatSettings & format_settings)
{
    if (const auto * map_col = typeid_cast<const ColumnMap *>(tags_column.get()))
    {
        const auto & array_column = map_col->getNestedColumn();
        const auto & offsets = array_column.getOffsets();
        const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column.getData());
        const auto & key_column = tuple_column.getColumn(0);
        const auto & value_column = tuple_column.getColumn(1);

        size_t start = row_index == 0 ? 0 : offsets[row_index - 1];
        size_t end = offsets[row_index];
        for (size_t j = start; j < end; ++j)
        {
            writeString(",", response);
            writeJSONString(key_column.getDataAt(j), response, format_settings);
            writeString(":", response);
            writeJSONString(value_column.getDataAt(j), response, format_settings);
        }
        return;
    }

    if (const auto * array_column = typeid_cast<const ColumnArray *>(tags_column.get()))
    {
        const auto & offsets = array_column->getOffsets();
        const auto & tuple_column = typeid_cast<const ColumnTuple &>(array_column->getData());
        const auto & key_column = tuple_column.getColumn(0);
        const auto & value_column = tuple_column.getColumn(1);

        size_t start = row_index == 0 ? 0 : offsets[row_index - 1];
        size_t end = offsets[row_index];
        for (size_t j = start; j < end; ++j)
        {
            writeString(",", response);
            writeJSONString(key_column.getDataAt(j), response, format_settings);
            writeString(":", response);
            writeJSONString(value_column.getDataAt(j), response, format_settings);
        }
    }
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
    const Params & params)
{
    PrometheusQueryEvaluationSettings evaluation_settings;
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, getContext())->getInMemoryMetadataPtr(getContext(), false);
    evaluation_settings.time_series_storage_id = time_series_storage->getStorageID();
    auto timestamp_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    evaluation_settings.timestamp_data_type = timestamp_data_type;
    evaluation_settings.scalar_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;

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

    PullingPipelineExecutor executor(io.pipeline);

    /// Mind using the getResultType() method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
    writeQueryResponse(response, executor, converter.getResultType());
}

void PrometheusHTTPProtocolAPI::writeQueryResponse(
    WriteBuffer & response, PullingPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type)
{
    /// Pull until the first non-empty block is ready before writing the header
    /// because pulling_executor.pull() can throw an exception and it's better to catch it early and write
    /// the correct error header {"status":"error", ...} in PrometheusRequestHandler::QueryAPIImpl.
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

void PrometheusHTTPProtocolAPI::getSeries(
    WriteBuffer & response,
    const std::vector<String> & match_params,
    const String & start_param,
    const String & end_param)
{
    const auto & ts_settings = time_series_storage->getStorageSettings();
    const Map & tags_to_columns_map = ts_settings[TimeSeriesSetting::tags_to_columns];
    const String time_where = buildTimeOverlapWhere(ts_settings, start_param, end_param);
    const String where_clause = buildMatchWhere(match_params, tags_to_columns_map, time_where);
    const UInt64 max_series = ts_settings[TimeSeriesSetting::prometheus_max_series];

    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    std::vector<String> select_list;
    select_list.push_back(TimeSeriesColumnNames::MetricName);
    select_list.push_back(TimeSeriesColumnNames::Tags);
    std::vector<std::pair<String, String>> promoted_tags;
    for (const auto & tag_name_and_column_name : tags_to_columns_map)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        select_list.push_back(backQuoteIfNeed(column_name));
        promoted_tags.emplace_back(tag_name, column_name);
    }

    String select_expr;
    for (size_t si = 0; si < select_list.size(); ++si)
    {
        if (si)
            select_expr += ", ";
        select_expr += select_list[si];
    }

    String query = fmt::format(
        "SELECT {} FROM {}{} LIMIT {}",
        select_expr,
        tags_table_id.getFullTableName(),
        where_clause,
        max_series);

    LOG_TRACE(log, "Prometheus series query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);
    Block result_block;

    writeString(R"({"status":"success","data":[)", response);

    bool first_row = true;
    while (executor.pull(result_block))
    {
        if (result_block.empty() || result_block.rows() == 0)
            continue;

        const auto & metric_name_col = result_block.getByName(TimeSeriesColumnNames::MetricName).column;
        const auto & tags_col = result_block.getByName(TimeSeriesColumnNames::Tags).column;

        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            if (!first_row)
                writeString(",", response);
            first_row = false;

            writeString(R"({"__name__":)", response);
            writeJSONString(metric_name_col->getDataAt(i), response, format_settings);

            for (const auto & [tag_name, column_name] : promoted_tags)
            {
                const auto & col = result_block.getByName(column_name).column;
                const auto value = col->getDataAt(i);
                /// Promoted columns store `''` for series that did not carry the label (column default).
                /// There is no presence bit to distinguish absent from explicit-empty, and Prometheus
                /// treats missing == empty, so do not synthesize the label in the response.
                if (value.empty())
                    continue;
                writeString(",", response);
                writeJSONString(std::string_view{tag_name}, response, format_settings);
                writeString(":", response);
                writeJSONString(value, response, format_settings);
            }

            writeJSONPairsFromTagsColumn(tags_col, i, response, format_settings);
            writeString("}", response);
        }
    }

    writeString("]}", response);
}

void PrometheusHTTPProtocolAPI::getLabels(
    WriteBuffer & response,
    const std::vector<String> & match_params,
    const String & start_param,
    const String & end_param)
{
    const auto & ts_settings = time_series_storage->getStorageSettings();
    const Map & tags_to_columns_map = ts_settings[TimeSeriesSetting::tags_to_columns];
    const String time_where = buildTimeOverlapWhere(ts_settings, start_param, end_param);
    const String where_clause = buildMatchWhere(match_params, tags_to_columns_map, time_where);

    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    const UInt64 max_series = ts_settings[TimeSeriesSetting::prometheus_max_series];

    /// Derive the label set from matched rows only; do not unconditionally add `__name__` or every promoted label
    /// from `tags_to_columns`, because `match[]` may exclude all series or matched series may not carry every
    /// promoted label.
    String labels_expr = "if(count() > 0, ['__name__'], CAST([], 'Array(String)'))";
    for (const auto & tag_name_and_column_name : tags_to_columns_map)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        labels_expr += fmt::format(
            ", if(countIf({} != '') > 0, [{}], CAST([], 'Array(String)'))",
            backQuoteIfNeed(column_name),
            quoteString(tag_name));
    }
    labels_expr += fmt::format(", groupUniqArrayArray(mapKeys({}))", TimeSeriesColumnNames::Tags);

    String query = fmt::format(
        "SELECT label_key FROM (SELECT arrayJoin(arrayDistinct(arrayConcat({}))) AS label_key FROM {}{}) LIMIT {}",
        labels_expr,
        tags_table_id.getFullTableName(),
        where_clause,
        max_series);

    LOG_TRACE(log, "Prometheus labels query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);
    Block result_block;

    std::set<String> label_names;
    while (executor.pull(result_block))
    {
        if (result_block.empty() || result_block.rows() == 0)
            continue;

        const auto & label_col = result_block.getByName("label_key").column;
        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            auto label = label_col->getDataAt(i);
            if (label.empty())
                continue;
            label_names.insert(String{label});
        }
    }

    writeString(R"({"status":"success","data":[)", response);
    bool first_label = true;
    for (const auto & name : label_names)
    {
        if (!first_label)
            writeString(",", response);
        first_label = false;
        writeJSONString(std::string_view{name}, response, format_settings);
    }
    writeString("]}", response);
}

void PrometheusHTTPProtocolAPI::getLabelValues(
    WriteBuffer & response,
    const String & label_name,
    const std::vector<String> & match_params,
    const String & start_param,
    const String & end_param)
{
    const auto & ts_settings = time_series_storage->getStorageSettings();
    const Map & tags_to_columns_map = ts_settings[TimeSeriesSetting::tags_to_columns];
    const String time_where = buildTimeOverlapWhere(ts_settings, start_param, end_param);
    const String where_clause = buildMatchWhere(match_params, tags_to_columns_map, time_where);

    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    const UInt64 max_series = ts_settings[TimeSeriesSetting::prometheus_max_series];
    String query;

    if (label_name == "__name__")
    {
        query = fmt::format(
            "SELECT label_value FROM (SELECT DISTINCT {} AS label_value FROM {}{}) ORDER BY label_value LIMIT {}",
            TimeSeriesColumnNames::MetricName,
            tags_table_id.getFullTableName(),
            where_clause,
            max_series);
    }
    else if (auto promoted_col = findColumnForTag(tags_to_columns_map, label_name))
    {
        /// A series without this promoted label is ingested as the column's default value (empty string for
        /// String/LowCardinality(String)). There is no presence bit to distinguish that from an explicit empty
        /// value, and Prometheus treats missing == empty, so excluding `''` is consistent with the documented
        /// label-values contract and with `/labels`, which also gates promoted labels on `countIf(col != '') > 0`.
        const auto col = backQuoteIfNeed(*promoted_col);
        const String presence_predicate = fmt::format("{} != ''", col);
        const String values_where
            = where_clause.empty() ? " WHERE " + presence_predicate : where_clause + " AND " + presence_predicate;
        query = fmt::format(
            "SELECT label_value FROM (SELECT DISTINCT {} AS label_value FROM {}{}) ORDER BY label_value LIMIT {}",
            col,
            tags_table_id.getFullTableName(),
            values_where,
            max_series);
    }
    else
    {
        const auto key_lit = quoteString(label_name);
        const auto map_access = fmt::format("{}[{}]", TimeSeriesColumnNames::Tags, key_lit);
        String map_where;
        if (where_clause.empty())
            map_where = fmt::format(" WHERE mapContains({}, {})", TimeSeriesColumnNames::Tags, key_lit);
        else
            map_where = where_clause + fmt::format(" AND mapContains({}, {})", TimeSeriesColumnNames::Tags, key_lit);
        /// DISTINCT keeps empty-string label values; `arrayJoin` drops empty strings in ClickHouse.
        query = fmt::format(
            "SELECT label_value FROM (SELECT DISTINCT {} AS label_value FROM {}{}) ORDER BY label_value LIMIT {}",
            map_access,
            tags_table_id.getFullTableName(),
            map_where,
            max_series);
    }

    LOG_TRACE(log, "Prometheus label values query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);
    Block result_block;

    writeString(R"({"status":"success","data":[)", response);

    bool first = true;
    while (executor.pull(result_block))
    {
        if (result_block.empty() || result_block.rows() == 0)
            continue;

        const auto & value_col = result_block.getByName("label_value").column;

        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            auto value = value_col->getDataAt(i);
            if (!first)
                writeString(",", response);
            first = false;
            writeJSONString(value, response, format_settings);
        }
    }

    writeString("]}", response);
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
