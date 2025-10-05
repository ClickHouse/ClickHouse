#include <Processors/Sources/ArrowFlightSource.h>

#if USE_ARROWFLIGHT
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ArrowFlight/ArrowFlightConnection.h>
#include <arrow/table.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ARROWFLIGHT_CONNECTION_FAILURE;
extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
extern const int ARROWFLIGHT_INTERNAL_ERROR;
}

ArrowFlightSource::ArrowFlightSource(
    std::shared_ptr<ArrowFlightConnection> connection_,
    const String & dataset_name_,
    const Block & sample_block_)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , connection(connection_)
    , sample_block(sample_block_)
{
    initializeEndpoints(dataset_name_);
}

ArrowFlightSource::ArrowFlightSource(
    std::shared_ptr<ArrowFlightConnection> connection_,
    std::vector<arrow::flight::FlightEndpoint> endpoints_,
    const Block & sample_block_)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , connection(connection_)
    , sample_block(sample_block_)
    , endpoints(std::move(endpoints_))
{
}

ArrowFlightSource::ArrowFlightSource(
    std::unique_ptr<arrow::flight::MetadataRecordBatchReader> stream_reader_,
    const Block & sample_block_)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , sample_block(sample_block_)
    , stream_reader(std::move(stream_reader_))
{
    initializeSchema();
}


ArrowFlightSource::~ArrowFlightSource() = default;


void ArrowFlightSource::initializeEndpoints(const String & dataset_name_)
{
    auto client = connection->getClient();
    auto options = connection->getOptions();

    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({dataset_name_});

    auto flight_info_res = client->GetFlightInfo(*options, descriptor);
    if (!flight_info_res.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get FlightInfo from Arrow Flight server: {}",
            flight_info_res.status().ToString());
    }

    std::unique_ptr<arrow::flight::FlightInfo> flight_info = std::move(flight_info_res).ValueOrDie();

    if (flight_info->endpoints().empty())
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "FlightInfo returned with no endpoints");

    endpoints = flight_info->endpoints();
}


bool ArrowFlightSource::nextEndpoint()
{
    if (current_endpoint >= endpoints.size())
        return false;

    const auto & endpoint = endpoints[current_endpoint];

    auto client = connection->getClient();
    auto options = connection->getOptions();

    arrow::flight::Ticket ticket = endpoint.ticket;

    auto stream_reader_res = client->DoGet(*options, ticket);
    if (!stream_reader_res.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE, "Failed to initialize Arrow Flight stream: {}", stream_reader_res.status().ToString());
    }

    stream_reader = std::move(stream_reader_res.ValueOrDie());
    initializeSchema();

    ++current_endpoint;
    return true;
}


void ArrowFlightSource::initializeSchema()
{
    if (stream_reader)
    {
        auto schema_res = stream_reader->GetSchema();
        if (!schema_res.ok())
            throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "Failed to get table schema: {}", schema_res.status().ToString());

        schema = schema_res.ValueOrDie();
    }
}


Chunk ArrowFlightSource::generate()
{
    arrow::flight::FlightStreamChunk chunk;
    while (!chunk.data)
    {
        if (!stream_reader && !nextEndpoint())
        {
            /// No more endpoints, we've read everything.
            return {};
        }

        chassert(stream_reader);

        auto chunk_res = stream_reader->Next();
        if (!chunk_res.ok())
            throw Exception(ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR, "Arrow Flight internal error: {}", chunk_res.status().ToString());

        chunk = chunk_res.ValueOrDie();

        if (!chunk.data)
        {
            /// We've finished reading from this stream reader, now it's time to try the next endpoint.
            stream_reader = nullptr;
        }
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

    auto table_res = arrow::Table::FromRecordBatches({chunk.data});
    if (!table_res.ok())
    {
        throw Exception(ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR, "Arrow Flight internal error: {}", table_res.status().ToString());
    }
    auto table = std::move(table_res).ValueOrDie();

    return converter.arrowTableToCHChunk(table, chunk.data->num_rows(), nullptr, nullptr);
}

}

#endif
