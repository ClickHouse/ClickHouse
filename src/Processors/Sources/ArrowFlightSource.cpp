#include "ArrowFlightSource.h"

#include <Processors/Chunk.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <arrow/array.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
    extern const int ARROWFLIGHT_INTERNAL_ERROR;
}

ArrowFlightSource::ArrowFlightSource(
    std::unique_ptr<arrow::flight::FlightClient> & client_,
    const std::string & query_,
    const Block & sample_block_,
    const std::vector<std::string> & column_names_,
    UInt64 /*max_block_size_*/)
    : ISource(sample_block_.cloneEmpty())
    , client(std::move(client_))
    , query(query_)
    , sample_block(sample_block_)
    , column_names(column_names_)
{
    initializeStream();
}

void ArrowFlightSource::initializeStream()
{
    arrow::flight::FlightCallOptions options;
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Command(query);

    auto flight_info_result = client->GetFlightInfo(options, descriptor);
    if (!flight_info_result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get FlightInfo from Arrow Flight server: {}",
            flight_info_result.status().ToString()
        );
    }

    std::unique_ptr<arrow::flight::FlightInfo> flight_info = std::move(flight_info_result).ValueOrDie();

    if (flight_info->endpoints().empty())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "FlightInfo returned with no endpoints"
        );
    }

    arrow::flight::Ticket ticket = flight_info->endpoints()[0].ticket;

    auto result = client->DoGet(options, ticket);
    if (!result.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE,
            "Failed to initialize Arrow Flight stream: {}",
            result.status().ToString()
        );
    }

    stream_reader = std::move(result.ValueOrDie());

    auto result_schema = stream_reader->GetSchema();
    if (!result_schema.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get table schema: {}",
            result_schema.status().ToString()
        );
    }
    schema = result_schema.ValueOrDie();
}

Chunk ArrowFlightSource::generate()
{
    auto status = stream_reader->Next();

    if (!status.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR,
            "Arrow Flight internal error: {}",
            status.status().ToString()
        );
    }

    auto chunk = status.ValueOrDie();
    auto batch = chunk.data;

    if (!batch)
    {
        return {};
    }

    MutableColumns columns;

    ArrowColumnToCHColumn converter(
        sample_block,
        "Arrow",
        true /* allow_missing_columns */,
        true /* null_as_default */,
        FormatSettings::DateTimeOverflowBehavior::Throw,
        false /* case_insensitive_matching */,
        false /* is_stream */
    );

    auto table = arrow::Table::FromRecordBatches({batch}).ValueOrDie();
    auto ch_chunk = converter.arrowTableToCHChunk(table, batch->num_rows());
    for (auto & col : ch_chunk.getColumns())
        columns.push_back(IColumn::mutate(col->cloneResized(batch->num_rows())));

    Chunk filled_chunk(std::move(columns), batch->num_rows());
    return filled_chunk;
}

}
