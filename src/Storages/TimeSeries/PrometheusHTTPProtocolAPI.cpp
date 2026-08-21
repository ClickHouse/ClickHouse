#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
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
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, getContext())->getInMemoryMetadataPtr();
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
    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);

    /// Mind using the getResultType() method from PrometheusQueryToSQL::Converter, not from the PrometheusQueryTree.
    writeQueryResponse(response, executor, converter.getResultType());
}

void PrometheusHTTPProtocolAPI::writeQueryResponse(
    WriteBuffer & response, PullingPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type)
{
    writeQueryResponseHeader(response, result_type);

    Block result_block;
    bool first = true;

    while (pulling_executor.pull(result_block))
    {
        if (result_block.rows() > 0)
        {   writeQueryResponseBlock(response, result_type, result_block, first);
            first = false;
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
    const String & /* match_param */,
    const String & /* start_param */,
    const String & /* end_param */)
{
    UNUSED(response);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The series endpoint is not implemented");
}

void PrometheusHTTPProtocolAPI::getLabels(
    WriteBuffer & response,
    const String & /* match_param */,
    const String & /* start_param */,
    const String & /* end_param */)
{
    UNUSED(response);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The labels endpoint is not implemented");
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
        if (value < 0)
            response.write('-');
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
