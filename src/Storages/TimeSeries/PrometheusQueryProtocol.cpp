#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL.h>
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
    extern const int BAD_REQUEST_PARAMETER;
    extern const int SYNTAX_ERROR;
}

PrometheusHTTPProtocolAPI::PrometheusHTTPProtocolAPI(ConstStoragePtr time_series_storage_, const ContextPtr & context_)
    : WithContext{context_}
    , time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , log(getLogger("PrometheusHTTPProtocolAPI"))
{
}

PrometheusHTTPProtocolAPI::~PrometheusHTTPProtocolAPI() = default;

void PrometheusHTTPProtocolAPI::executeInstantQuery(
    WriteBuffer & response,
    const String & promql_query,
    const String & time_param)
{
    try
    {
        LOG_DEBUG(log, "Executing instant query: {} at time: {}", promql_query, time_param);

        // Parse the timestamp parameter
        Field evaluation_time = parseTimestamp(time_param);

        // Parse the PromQL query
        auto query_tree = parsePromQLQuery(promql_query);
        if (!query_tree)
        {
            writeErrorResponse(response, "bad_data", "Failed to parse PromQL query");
            return;
        }
        LOG_DEBUG(log, "Parsed PromQL query: {}. Result type: {}", promql_query, query_tree->getResultType());

        // Create TimeSeriesTableInfo structure
        PrometheusQueryToSQLConverter::TimeSeriesTableInfo table_info;
        table_info.storage_id = time_series_storage->getStorageID();
        table_info.timestamp_data_type = std::make_shared<DataTypeDateTime64>(3); // millisecond precision
        table_info.value_data_type = std::make_shared<DataTypeFloat64>();

        // Convert PromQL to SQL
        PrometheusQueryToSQLConverter converter(
            *query_tree,
            table_info,
            Field(300.0), // lookback_delta - 5 minutes default
            Field(15.0)   // default_resolution - 15 seconds
        );

        converter.setEvaluationTime(evaluation_time);

        auto sql_query = converter.getSQL();
        if (!sql_query)
        {
            writeErrorResponse(response, "execution", "Failed to convert PromQL to SQL");
            return;
        }

        // Execute the SQL query
        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();

        // Set a unique query ID (required for ProcessList)
        query_context->setCurrentQueryId(toString(thread_local_rng()));

        auto [ast, io] = executeQuery(sql_query->formatForErrorMessage(), query_context, {}, QueryProcessingStage::Complete);

        if (query_tree->getResultType() == PrometheusQueryTree::ResultType::RANGE_VECTOR)
        {
            PullingPipelineExecutor executor(io.pipeline);
            Block result_block;

            writeRangeQueryHeader(response);
            while (executor.pull(result_block))
                writeRangeQueryResponse(response, result_block);
            writeRangeQueryFooter(response);
            return;
        }

        // Read the result
        PullingPipelineExecutor executor(io.pipeline);
        Block result_block;
        while (executor.pull(result_block))
        {
            if (!result_block.empty())
            {
                writeInstantQueryResponse(response, result_block);
                return;
            }
        }

        // If no results, return empty vector
        writeString(R"({"status":"success","data":{"resultType":"vector","result":[]}})", response);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Error executing instant query: {}", e.displayText());
        writeErrorResponse(response, "execution", e.message());
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknown error executing instant query");
        writeErrorResponse(response, "internal", "Internal server error");
    }
}

void PrometheusHTTPProtocolAPI::executeRangeQuery(
    WriteBuffer & response,
    const String & promql_query,
    const String & start_param,
    const String & end_param,
    const String & step_param)
{
    try
    {
        LOG_DEBUG(log, "Executing range query: {} from {} to {} step {}", promql_query, start_param, end_param, step_param);

        Field start_time = parseTimestamp(start_param);
        Field end_time = parseTimestamp(end_param);
        Field step = parseStep(step_param);

        // Parse the PromQL query
        auto query_tree = parsePromQLQuery(promql_query);
        if (!query_tree)
        {
            writeErrorResponse(response, "bad_data", "Failed to parse PromQL query");
            return;
        }

        LOG_DEBUG(log, "Parsed PromQL query: {}. Result type: {}", promql_query, query_tree->getResultType());

        // Create TimeSeriesTableInfo structure
        PrometheusQueryToSQLConverter::TimeSeriesTableInfo table_info;
        table_info.storage_id = time_series_storage->getStorageID();
        table_info.timestamp_data_type = std::make_shared<DataTypeDateTime64>(3); // millisecond precision
        table_info.value_data_type = std::make_shared<DataTypeFloat64>();

        Field lookback_delta = Field(end_time.safeGet<Float64>() - start_time.safeGet<Float64>());

        PrometheusQueryToSQLConverter converter
        (
            *query_tree,
            table_info,
            lookback_delta, // lookback_delta based on time range
            step          // resolution (step parameter)
        );
        converter.setEvaluationRange({start_time, end_time, step});

        auto sql_query = converter.getSQL();
        if (!sql_query)
        {
            writeErrorResponse(response, "execution", "Failed to convert PromQL to SQL");
            return;
        }

        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->setCurrentQueryId(toString(thread_local_rng()));
        query_context->setSetting("allow_experimental_time_series_aggregate_functions", Field(UInt64(1)));

        auto [ast2, io] = executeQuery(sql_query->formatForErrorMessage(), query_context, {}, QueryProcessingStage::Complete);

        PullingPipelineExecutor executor(io.pipeline);
        Block result_block;

        writeRangeQueryHeader(response);
        while (executor.pull(result_block))
            writeRangeQueryResponse(response, result_block);
        writeRangeQueryFooter(response);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Error executing range query: {}", e.displayText());
        writeErrorResponse(response, "execution", e.message());
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknown error executing range query");
        writeErrorResponse(response, "internal", "Internal server error");
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

Field PrometheusHTTPProtocolAPI::parseTimestamp(const String & time_param)
{
    if (time_param.empty())
        return Field(static_cast<Float64>(time(nullptr))); // Current time as default

    // Try to parse as Unix timestamp
    try
    {
        Float64 timestamp = std::stod(time_param);
        return Field(timestamp);
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Invalid timestamp format: {}", time_param);
    }
}

Field PrometheusHTTPProtocolAPI::parseStep(const String & step_param)
{
    if (step_param.empty())
        return Field(15.0); // Default 15 seconds

    try
    {
        // Parse step parameter (e.g., "15s", "1m", "1h")
        if (step_param.ends_with("s"))
        {
            String num_str = step_param.substr(0, step_param.length() - 1);
            return Field(std::stod(num_str));
        }
        else if (step_param.ends_with("m"))
        {
            String num_str = step_param.substr(0, step_param.length() - 1);
            return Field(std::stod(num_str) * 60);
        }
        else if (step_param.ends_with("h"))
        {
            String num_str = step_param.substr(0, step_param.length() - 1);
            return Field(std::stod(num_str) * 3600);
        }
        else
        {
            // Assume seconds if no unit
            return Field(std::stod(step_param));
        }
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Invalid step format: {}", step_param);
    }
}

std::unique_ptr<PrometheusQueryTree> PrometheusHTTPProtocolAPI::parsePromQLQuery(const String & promql_query)
{
    try
    {
        // Parse the PromQL query string into an AST
        auto query_tree = std::make_unique<PrometheusQueryTree>();

        // For now, we'll create a simple query tree that represents a basic metric query
        // In a full implementation, this would parse the full PromQL syntax
        if (!promql_query.empty())
        {
            // Basic parsing - this is a placeholder for full PromQL parsing
            query_tree->parse(promql_query);
        }

        return query_tree;
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to parse PromQL query '{}': {}", promql_query, e.displayText());
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to parse PromQL query: {}", e.message());
    }
}

void DB::PrometheusHTTPProtocolAPI::writeInstantQueryResponse(WriteBuffer & response, const Block & result_block)
{

    LOG_INFO(log, "Prometheus: Writing instant query response");
    // Start JSON response
    writeString(R"({"status":"success","data":{)", response);

    LOG_INFO(log, "Prometheus block: {}", result_block.dumpStructure());

    // Determine result type based on block structure
    bool is_scalar = !result_block.empty() && result_block.has(TimeSeriesColumnNames::Scalar) && !result_block.has(TimeSeriesColumnNames::Tags);

    if (is_scalar)
    {
        writeString(R"("resultType":"scalar","result":)", response);
        writeScalarResult(response, result_block);
    }
    else
    {
        writeString(R"("resultType":"vector","result":)", response);
        writeVectorResult(response, result_block);
    }

    // Close JSON response
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
                writeFloatText(std::round(time(nullptr) * 100.0) / 100.0, response);
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
                    String key = key_column.getDataAt(j).toString();

                    if (!first)
                        writeString(",", response);
                    first = false;

                    writeString("\"", response);
                    writeString(key, response);
                    writeString("\":\"", response);
                    writeString(value_column.getDataAt(j).toString(), response);
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
                        writeFloatText(std::round(timestamp_column.getFloat64(j)), response);
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

void DB::PrometheusHTTPProtocolAPI::writeErrorResponse(WriteBuffer & response, const String & error_type, const String & error_message)
{
    writeString(R"({"status":"error","errorType":")", response);
    writeString(error_type, response);
    writeString(R"(","error":")", response);
    writeString(error_message, response);
    writeString(R"("})", response);
}

}
