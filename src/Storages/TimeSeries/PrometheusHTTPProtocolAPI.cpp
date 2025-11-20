#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Core/Field.h>
#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/StorageTimeSeries.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>
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
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, getContext())->getInMemoryMetadataPtr();
    UInt32 time_scale = PrometheusQueryToSQL::getResultTimeScale(data_table_metadata);

    PrometheusQueryTree query_tree{params.promql_query};
    LOG_TRACE(log, "Parsed PromQL query: {}. Result type: {}", params.promql_query, query_tree.getResultType());

    PrometheusQueryEvaluationSettings evaluation_settings;
    evaluation_settings.time_series_storage_id = time_series_storage->getStorageID();
    evaluation_settings.data_table_metadata = data_table_metadata;

    if (params.type == Type::Instant)
    {
        evaluation_settings.evaluation_time = getTimeseriesTime(Field{params.time_param}, time_scale);
    }
    else if (params.type == Type::Range)
    {
        evaluation_settings.evaluation_range = PrometheusQueryEvaluationRange{
            .start_time = getTimeseriesTime(Field{params.start_param}, time_scale),
            .end_time = getTimeseriesTime(Field{params.end_param}, time_scale),
            .step = getTimeseriesDuration(Field{params.step_param}, time_scale)};
    }

    PrometheusQueryToSQL::Converter converter(std::make_shared<PrometheusQueryTree>(std::move(query_tree)), evaluation_settings);

    auto sql_query = converter.getSQL();
    chassert(sql_query);

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
    chassert(!result_block.empty());
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
        const auto & time_column = result_block.getByName(TimeSeriesColumnNames::Time).column;
        auto time = time_column->getFloat64(0);
        writeString(std::to_string(time), response);

        writeString(",", response);

        // Write value
        const auto & scalar_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
        auto value = scalar_column->getFloat64(0);
        writeString("\"", response);
        writeFloatText(std::round(value * 100.0) / 100.0, response);
        writeString("\"", response);
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
            const auto & time_column = result_block.getByName(TimeSeriesColumnNames::Time).column;
            auto time = time_column->getFloat64(i);
            writeFloatText(std::round(time * 100.0) / 100.0, response);

            writeString(",", response);

            // Write value
            const auto & value_column = result_block.getByName(TimeSeriesColumnNames::Value).column;
            auto value = value_column->getFloat64(i);
            writeString("\"", response);
            writeFloatText(std::round(value * 100.0) / 100.0, response);
            writeString("\"", response);

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
