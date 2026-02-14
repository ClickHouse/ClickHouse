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
    extern const int BAD_ARGUMENTS;
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
    auto query_tree = std::make_shared<PrometheusQueryTree>();
    query_tree->parse(params.promql_query);
    if (!query_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse PromQL query");

    LOG_TRACE(log, "Parsed PromQL query: {}. Result type: {}", params.promql_query, query_tree->getResultType());

    // Create TimeSeriesTableInfo structure
    PrometheusQueryEvaluationSettings evaluation_settings;
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, getContext())->getInMemoryMetadataPtr();
    evaluation_settings.time_series_storage_id = time_series_storage->getStorageID();
    auto timestamp_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = tryGetDecimalScale(*timestamp_data_type).value_or(0);
    evaluation_settings.timestamp_data_type = timestamp_data_type;
    evaluation_settings.scalar_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;

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
    if (!sql_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to convert PromQL to SQL");

    auto [ast, io] = executeQuery(sql_query->formatWithSecretsOneLine(), getContext(), {}, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);
    Block result_block;

    /// Mind using the getResultType() method from PrometheusQueryToSQLConverter, not from the PrometheusQueryTree.
    if (converter.getResultType() == PrometheusQueryTree::ResultType::RANGE_VECTOR)
    {
        writeRangeQueryHeader(response);
        while (executor.pull(result_block))
            writeRangeQueryResponse(response, result_block);
        writeRangeQueryFooter(response);
        return;
    }
    else if (converter.getResultType() == PrometheusQueryTree::ResultType::INSTANT_VECTOR)
    {
        writeInstantQueryHeader(response);
        while (executor.pull(result_block))
            writeInstantQueryResponse(response, result_block);
        writeInstantQueryFooter(response);
        return;
    }
    else if (converter.getResultType() == PrometheusQueryTree::ResultType::SCALAR)
    {
        writeInstantQueryHeader(response);
        while (executor.pull(result_block))
            writeScalarQueryResponse(response, result_block);
        writeInstantQueryFooter(response);
        return;
    }
    else
    {
        LOG_ERROR(log, "Unsupported result type: {}", converter.getResultType());
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported result type");
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

void DB::PrometheusHTTPProtocolAPI::writeInstantQueryHeader(WriteBuffer & response)
{
    writeString(R"({"status":"success","data":{)", response);
}

void DB::PrometheusHTTPProtocolAPI::writeScalarQueryResponse(WriteBuffer & response, const Block & result_block)
{
    chassert(!result_block.empty() && result_block.has(TimeSeriesColumnNames::Scalar) && !result_block.has(TimeSeriesColumnNames::Tags));
    writeString(R"("resultType":"scalar","result":)", response);
    writeScalarResult(response, result_block);
}

void DB::PrometheusHTTPProtocolAPI::writeInstantQueryResponse(WriteBuffer & response, const Block & result_block)
{
    writeString(R"("resultType":"vector","result":)", response);
    writeVectorResult(response, result_block);
}

void DB::PrometheusHTTPProtocolAPI::writeInstantQueryFooter(WriteBuffer & response)
{
    writeString("}}", response);
}

void DB::PrometheusHTTPProtocolAPI::writeScalarResult(WriteBuffer & response, const Block & result_block)
{
    LOG_INFO(log, "Prometheus: Writing scalar result");

    writeString("[", response);

    if (!result_block.empty() && result_block.rows() > 0)
    {
        // Write timestamp
        if (result_block.has(TimeSeriesColumnNames::Timestamp))
        {
            const auto & ts_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
            auto timestamp = ts_column->getFloat64(0);
            writeString(std::to_string(timestamp), response);
        }
        else
        {
            writeString(std::to_string(time(nullptr)), response);
        }

        writeString(",", response);

        // Write value
        if (result_block.has(TimeSeriesColumnNames::Scalar))
        {
            const auto & scalar_column = result_block.getByName(TimeSeriesColumnNames::Scalar).column;
            auto value = scalar_column->getFloat64(0);
            writeString("\"", response);
            writeFloatText(std::round(value * 100.0) / 100.0, response);
            writeString("\"", response);
        }
        else
        {
            writeString("\"0\"", response);
        }
    }

    writeString("]", response);
}

void DB::PrometheusHTTPProtocolAPI::writeVectorResult(WriteBuffer & response, const Block & result_block)
{
    writeString("[", response);

    if (!result_block.empty() && result_block.rows() > 0)
    {
        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            if (i > 0)
                writeString(",", response);

            writeString("{", response);

            // Write metric labels
            writeString(R"("metric":)", response);
            writeMetricLabels(response, result_block, i);

            writeString(",", response);

            // Write value [timestamp, "value"]
            writeString("\"value\":[", response);

            // Write timestamp
            if (result_block.has(TimeSeriesColumnNames::Timestamp))
            {
                const auto & ts_column = result_block.getByName(TimeSeriesColumnNames::Timestamp).column;
                auto timestamp = ts_column->getFloat64(i);
                writeFloatText(std::round(timestamp * 100.0) / 100.0, response);
            }
            else
            {
                writeFloatText(std::round(static_cast<double>(time(nullptr)) * 100.0) / 100.0, response);
            }

            writeString(",", response);

            // Write value
            if (result_block.has(TimeSeriesColumnNames::Value))
            {
                const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
                auto value = value_column->getFloat64(i);
                writeString("\"", response);
                writeFloatText(std::round(value * 100.0) / 100.0, response);
                writeString("\"", response);
            }
            else if (result_block.has(TimeSeriesColumnNames::Scalar))
            {
                const auto & scalar_column = result_block.getByName(TimeSeriesColumnNames::Scalar).column;
                auto value = scalar_column->getFloat64(i);
                writeString("\"", response);
                writeFloatText(std::round(value * 100.0) / 100.0, response);
                writeString("\"", response);
            }
            else
            {
                writeString("\"0\"", response);
            }

            writeString("]}", response);
        }
    }

    writeString("]", response);
}

void DB::PrometheusHTTPProtocolAPI::writeMetricLabels(WriteBuffer & response, const Block & result_block, size_t row_index)
{
    writeString("{", response);

    if (result_block.has(TimeSeriesColumnNames::Tags))
    {
        const auto & tags_column = result_block.getByName(TimeSeriesColumnNames::Tags).column;
        if (const auto * array_column = typeid_cast<const ColumnArray *>(tags_column.get()))
        {
            const auto & offsets = array_column->getOffsets();
            size_t start = (row_index == 0) ? 0 : offsets[row_index - 1];
            size_t end = offsets[row_index];

            if (const auto * tuple_column = typeid_cast<const ColumnTuple *>(&array_column->getData()))
            {
                const auto & key_column = tuple_column->getColumn(0);
                const auto & value_column = tuple_column->getColumn(1);

                bool first = true;
                for (size_t j = start; j < end; ++j)
                {
                    String key{key_column.getDataAt(j)};

                    if (!first)
                        writeString(",", response);
                    first = false;

                    writeString("\"", response);
                    writeString(key, response);
                    writeString("\":\"", response);
                    writeString(value_column.getDataAt(j), response);
                    writeString("\"", response);
                }
            }
        }
    }

    writeString("}", response);
}

void DB::PrometheusHTTPProtocolAPI::writeRangeQueryHeader(WriteBuffer & response)
{
    writeString(R"({"status":"success","data":{"resultType":"matrix","result":[)", response);
}

void DB::PrometheusHTTPProtocolAPI::writeRangeQueryFooter(WriteBuffer & response)
{
    writeString(R"(]}})", response);
}

void DB::PrometheusHTTPProtocolAPI::writeRangeQueryResponse(WriteBuffer & response, const Block & result_block)
{
    if (!result_block.empty() && result_block.rows() > 0)
    {
        // For range queries, we need to group results by metric labels
        // This is a simplified implementation
        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            if (i > 0)
                writeString(",", response);

            writeString("{", response);

            // Write metric labels using the shared function that skips __name__
            writeString(R"("metric":)", response);
            writeMetricLabels(response, result_block, i);
            writeString(",", response);

            // Extract time series data
            writeString(R"("values":[)", response);


            const auto & ts_column = result_block.getByName(TimeSeriesColumnNames::TimeSeries).column;
            if (const auto * array_column = typeid_cast<const ColumnArray *>(ts_column.get()))
            {
                const auto & offsets = array_column->getOffsets();
                size_t start = (i == 0) ? 0 : offsets[i-1];
                size_t end = offsets[i];

                if (const auto * tuple_column = typeid_cast<const ColumnTuple *>(&array_column->getData()))
                {
                    const auto & timestamp_column = tuple_column->getColumn(0);
                    const auto & value_column = tuple_column->getColumn(1);

                    for (size_t j = start; j < end; ++j)
                    {
                        if (j > start)
                            writeString(",", response);

                        writeString("[", response);
                        writeFloatText(timestamp_column.getFloat64(j), response);
                        writeString(",\"", response);
                        writeFloatText(std::round(value_column.getFloat64(j) * 100.0) / 100.0, response);
                        writeString("\"]", response);
                    }
                }
            }

            writeString("]}", response);
        }
    }
}

void DB::PrometheusHTTPProtocolAPI::writeSeriesResponse(WriteBuffer & response, const Block & /* result_block */)
{
    writeString(R"({"status":"success","data":[]})", response);
}

void DB::PrometheusHTTPProtocolAPI::writeLabelsResponse(WriteBuffer & response, const Block & /* result_block */)
{
    writeString(R"({"status":"success","data":["__name__","job","instance"]})", response);
}

void DB::PrometheusHTTPProtocolAPI::writeLabelValuesResponse(WriteBuffer & response, const Block & /* result_block */)
{
    writeString(R"({"status":"success","data":[]})", response);
}

}
