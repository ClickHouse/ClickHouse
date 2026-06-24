#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>
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
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
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

    PullingPipelineExecutor executor(io.pipeline);

    /// Mind using the getResultType() method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
    writeQueryResponse(response, executor, converter.getResultType());
}

void PrometheusHTTPProtocolAPI::writeQueryResponse(
    WriteBuffer & response, PullingPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type)
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

    auto tags_metadata = tags_table->getInMemoryMetadataPtr(getContext(), false);
    if (!tags_metadata->columns.has(TimeSeriesColumnNames::MinTime) || !tags_metadata->columns.has(TimeSeriesColumnNames::MaxTime))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot apply the 'start'/'end' time range on the Prometheus metadata endpoints because the 'tags' table has no "
            "'{}'/'{}' columns. Enable the 'store_min_time_and_max_time' setting of the TimeSeries table to use time range filtering",
            TimeSeriesColumnNames::MinTime,
            TimeSeriesColumnNames::MaxTime);

    /// `min_time`/`max_time` are `DateTime64(3)`. A series overlaps the requested range [start, end]
    /// when its `min_time <= end` and `max_time >= start`. NULL bounds mean "unknown", so such rows are
    /// kept to avoid hiding series whose time bounds are not set.
    static constexpr UInt32 time_scale = 3;

    if (!start_param.empty())
    {
        Int64 start_ms = parseTimeSeriesTimestamp(start_param, time_scale).value;
        conditions.push_back(fmt::format(
            "({0} IS NULL OR {0} >= fromUnixTimestamp64Milli(toInt64({1})))", TimeSeriesColumnNames::MaxTime, start_ms));
    }

    if (!end_param.empty())
    {
        Int64 end_ms = parseTimeSeriesTimestamp(end_param, time_scale).value;
        conditions.push_back(fmt::format(
            "({0} IS NULL OR {0} <= fromUnixTimestamp64Milli(toInt64({1})))", TimeSeriesColumnNames::MinTime, end_ms));
    }
}

/// Implements /api/v1/series: returns time series matching a metric name filter.
/// Queries the tags table and serializes each series as a JSON object with __name__ and all tag key-value pairs.
void PrometheusHTTPProtocolAPI::getSeries(
    WriteBuffer & response,
    const String & match_param,
    const String & start_param,
    const String & end_param)
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
    if (!match_param.empty())
    {
        /// Simple metric name matching: match[] parameter can be a metric name or {label=value} selector.
        /// For now, support plain metric name matching.
        conditions.push_back(fmt::format("{} = {}", TimeSeriesColumnNames::MetricName, quoteString(match_param)));
    }
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

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

        std::vector<const IColumn *> tag_value_cols;
        tag_value_cols.reserve(tag_columns.size());
        for (size_t c = 0; c < tag_columns.size(); ++c)
            tag_value_cols.push_back(result_block.getByName(fmt::format("__tsc_{}", c)).column.get());

        for (size_t i = 0; i < result_block.rows(); ++i)
        {
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
    }

    writeString("]}", response);
}

/// Implements /api/v1/labels: returns all distinct label names across all time series.
/// Always includes "__name__" as a virtual label, then queries distinct keys from the tags Map column.
void PrometheusHTTPProtocolAPI::getLabels(
    WriteBuffer & response,
    const String & match_param,
    const String & start_param,
    const String & end_param)
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
    if (!match_param.empty())
        conditions.push_back(fmt::format("{} = {}", TimeSeriesColumnNames::MetricName, quoteString(match_param)));
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    query += " ORDER BY label_key";

    LOG_TRACE(log, "Prometheus labels query: {}", query);

    auto [ast, io] = executeQuery(query, getContext(), {}, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);
    Block result_block;

    writeString(R"({"status":"success","data":["__name__")", response);

    while (executor.pull(result_block))
    {
        if (result_block.empty() || result_block.rows() == 0)
            continue;

        const auto & label_col = result_block.getByName("label_key").column;

        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            auto label = label_col->getDataAt(i);
            /// Skip __name__ since we already included it
            if (label == "__name__")
                continue;
            writeString(",", response);
            writeJSONString(label, response, format_settings);
        }
    }

    writeString("]}", response);
}

/// Implements /api/v1/label/<name>/values: returns all distinct values for a given label name.
/// For "__name__", queries the metric_name column directly; for tags moved into dedicated columns by
/// `tags_to_columns`, reads that column; otherwise extracts values from the tags Map.
void PrometheusHTTPProtocolAPI::getLabelValues(
    WriteBuffer & response,
    const String & label_name,
    const String & match_param,
    const String & start_param,
    const String & end_param)
{
    auto tags_table = time_series_storage->getTargetTable(ViewTarget::Tags, getContext());
    auto tags_table_id = tags_table->getStorageID();

    String query;
    /// Collect WHERE conditions and join them, so the query stays valid regardless of which branch is taken.
    std::vector<String> conditions;

    if (label_name == "__name__")
    {
        /// __name__ maps to the metric_name column directly
        query = fmt::format(
            "SELECT DISTINCT {} AS label_value FROM {}",
            TimeSeriesColumnNames::MetricName,
            tags_table_id.getFullTableName());
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
            /// Extract distinct values for a specific key from the tags Map
            query = fmt::format(
                "SELECT DISTINCT {}[{}] AS label_value FROM {}",
                TimeSeriesColumnNames::Tags,
                quoteString(label_name),
                tags_table_id.getFullTableName());
            conditions.push_back(fmt::format("mapContains({}, {})",
                TimeSeriesColumnNames::Tags,
                quoteString(label_name)));
        }
    }

    if (!match_param.empty())
    {
        conditions.push_back(fmt::format("{} = {}",
            TimeSeriesColumnNames::MetricName,
            quoteString(match_param)));
    }
    appendTimeRangeConditions(conditions, tags_table, start_param, end_param);

    for (size_t i = 0; i < conditions.size(); ++i)
        query += (i == 0 ? " WHERE " : " AND ") + conditions[i];

    query += " ORDER BY label_value";

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
            if (value.empty())
                continue;
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
