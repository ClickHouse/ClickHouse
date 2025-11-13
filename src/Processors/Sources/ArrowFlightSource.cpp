#include <Processors/Sources/ArrowFlightSource.h>

#if USE_ARROWFLIGHT
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <arrow/array.h>
#include <base/range.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ARROWFLIGHT_CONNECTION_FAILURE;
extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
extern const int ARROWFLIGHT_INTERNAL_ERROR;
}

ArrowFlightSource::ArrowFlightSource(
    const FlightClientPtr & client_,
    const std::string & query_,
    const Block & sample_block_,
    const std::vector<std::string> & column_names_,
    UInt64 /*max_block_size_*/)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , client(client_)
    , query(query_)
    , sample_block(sample_block_)
    , column_names(column_names_)
{
    initializeStream();
}

void ArrowFlightSource::initializeStream()
{
    arrow::flight::FlightCallOptions options;
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({query});

    auto flight_info_result = client->GetFlightInfo(options, descriptor);
    if (!flight_info_result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get FlightInfo from Arrow Flight server: {}",
            flight_info_result.status().ToString());
    }

    std::unique_ptr<arrow::flight::FlightInfo> flight_info = std::move(flight_info_result).ValueOrDie();

    if (flight_info->endpoints().empty())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "FlightInfo returned with no endpoints");
    }

    arrow::flight::Ticket ticket = flight_info->endpoints()[0].ticket;

    auto result = client->DoGet(options, ticket);
    if (!result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to initialize Arrow Flight stream: {}", result.status().ToString());
    }

    stream_reader = std::move(result.ValueOrDie());

    auto result_schema = stream_reader->GetSchema();
    if (!result_schema.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", result_schema.status().ToString());
    }
    schema = result_schema.ValueOrDie();
}

Chunk ArrowFlightSource::generate()
{
    auto status = stream_reader->Next();

    if (!status.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR, "Arrow Flight internal error: {}", status.status().ToString());
    }

    const auto & chunk = status.ValueOrDie();
    auto batch = chunk.data;

    if (!batch)
    {
        return {};
    }

    MutableColumns columns;

    ArrowColumnToCHColumn converter(sample_block, "Arrow",
                                    /* format_settings= */ {},
                                    /* parquet_columns_to_clickhouse= */ std::nullopt,
                                    /* clickhouse_columns_to_parquet= */ std::nullopt,
                                    /* allow_missing_columns = */ true,
                                    /* null_as_default = */ true,
                                    FormatSettings::DateTimeOverflowBehavior::Throw,
                                    /* allow_geoparquet_parser = */ false);

    auto batch_result = arrow::Table::FromRecordBatches({batch});
    if (!batch_result.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR, "Arrow Flight internal error: {}", batch_result.status().ToString());
    }
    auto table = std::move(batch_result).ValueOrDie();
    auto ch_chunk = converter.arrowTableToCHChunk(table, batch->num_rows(), nullptr, nullptr);
    for (const auto & col : ch_chunk.getColumns())
        columns.push_back(IColumn::mutate(col->cloneResized(batch->num_rows())));

    Chunk filled_chunk(std::move(columns), batch->num_rows());
    return filled_chunk;
}

}

#endif
