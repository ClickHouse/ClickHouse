#include <Common/VectorWithMemoryTracking.h>
#include <Processors/Sources/ArrowFlightSource.h>

#if USE_ARROWFLIGHT
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ArrowFlight/ArrowFlightConnection.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARROWFLIGHT_CONNECTION_FAILURE;
    extern const int ARROWFLIGHT_FETCH_SCHEMA_ERROR;
    extern const int ARROWFLIGHT_INTERNAL_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsArrowFlightDescriptorType arrow_flight_request_descriptor_type;
    extern const SettingsUInt64 arrow_flight_request_timeout_sec;
}

namespace
{

UInt64 getRequestTimeoutSec(const ContextPtr & context)
{
    if (!context)
        return 0;
    return context->getSettingsRef()[Setting::arrow_flight_request_timeout_sec];
}

Block convertBlockToHeader(Block block, const Block & header, ContextPtr context)
{
    auto converting_dag = ActionsDAG::makeConvertingActions(
        block.cloneEmpty().getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    auto actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    actions->execute(block);
    return block;
}

Block buildOutputHeader(const Block & sample_block_, const Block & virtual_header_)
{
    Block output_header = sample_block_.cloneEmpty();
    for (const auto & column : virtual_header_)
        output_header.insert(column.cloneEmpty());

    return output_header;
}

/// The columns of the sample block that are not in the data are filled with defaults, and NULLs of non-nullable
/// columns become defaults.
FormatSettings readSettings()
{
    FormatSettings settings;
    settings.arrow.allow_missing_columns = true;
    settings.null_as_default = true;
    settings.date_time_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Throw;
    return settings;
}

}

/// The messages being read: the buffer the reader reads them through, and the reader.
struct ArrowFlightSource::Stream
{
    /// The `DoGet` call the messages come from, if any. Declared first: the reader reads from it.
    std::shared_ptr<grpc::ClientContext> call_context;
    std::unique_ptr<grpc::ClientReader<arrow::flight::protocol::FlightData>> reader;
    bool finished = false;

    std::unique_ptr<ArrowIPC::IPCMessagesReadBuffer> buffer;
    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;

    ~Stream()
    {
        executor.reset();
        pipeline.reset();
        /// A call has to be finished; one that is not read to the end is cancelled first.
        if (reader && !finished)
        {
            call_context->TryCancel();
            reader->Finish();
        }
    }
};

ArrowFlightSource::ArrowFlightSource(
    std::shared_ptr<ArrowFlightConnection> connection_,
    const String & dataset_name_,
    const Block & sample_block_,
    const Block & virtual_header_,
    ContextPtr context_)
    : ISource(std::make_shared<const Block>(buildOutputHeader(sample_block_, virtual_header_)))
    , connection(connection_)
    , sample_block(sample_block_)
    , virtual_header(virtual_header_)
    , context(context_)
    , request_timeout_sec(getRequestTimeoutSec(context_))
{
    initializeEndpoints(dataset_name_);
}

ArrowFlightSource::ArrowFlightSource(
    std::shared_ptr<ArrowFlightConnection> connection_,
    VectorWithMemoryTracking<String> tickets_,
    const Block & sample_block_,
    ContextPtr context_)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , connection(connection_)
    , sample_block(sample_block_)
    , context(context_)
    , request_timeout_sec(getRequestTimeoutSec(context_))
    , tickets(std::move(tickets_))
{
}

ArrowFlightSource::ArrowFlightSource(
    NextMessage next_message_,
    const Block & sample_block_,
    ContextPtr context_)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , sample_block(sample_block_)
    , context(context_)
{
    startReading(std::move(next_message_));
}

ArrowFlightSource::~ArrowFlightSource() = default;

void ArrowFlightSource::startReading(NextMessage next_message_)
{
    auto new_stream = std::make_unique<Stream>();
    new_stream->buffer = std::make_unique<ArrowIPC::IPCMessagesReadBuffer>(std::move(next_message_));
    auto input = std::make_shared<ArrowIPCBlockInputFormat>(
        *new_stream->buffer, std::make_shared<const Block>(sample_block), /* stream_ */ true, DEFAULT_BLOCK_SIZE, readSettings());
    new_stream->pipeline = QueryPipeline(Pipe(std::move(input)));
    new_stream->executor = std::make_unique<PullingPipelineExecutor>(new_stream->pipeline);
    stream = std::move(new_stream);
}

void ArrowFlightSource::initializeEndpoints(const String & dataset_name_)
{
    auto stub = connection->getStub(request_timeout_sec);

    arrow::flight::protocol::FlightDescriptor descriptor;
    if (context && context->getSettingsRef()[Setting::arrow_flight_request_descriptor_type] == ArrowFlightDescriptorType::Command)
    {
        descriptor.set_type(arrow::flight::protocol::FlightDescriptor::CMD);
        descriptor.set_cmd("SELECT * FROM " + dataset_name_);
    }
    else
    {
        descriptor.set_type(arrow::flight::protocol::FlightDescriptor::PATH);
        descriptor.add_path(dataset_name_);
    }

    grpc::ClientContext call;
    connection->prepareCall(call, request_timeout_sec);
    arrow::flight::protocol::FlightInfo flight_info;
    auto status = stub->GetFlightInfo(&call, descriptor, &flight_info);
    if (!status.ok())
    {
        throw Exception(
            ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR,
            "Failed to get FlightInfo from Arrow Flight server: {}",
            ArrowFlightConnection::formatError(status, call));
    }

    if (flight_info.endpoint().empty())
        throw Exception(ErrorCodes::ARROWFLIGHT_FETCH_SCHEMA_ERROR, "FlightInfo returned with no endpoints");

    for (const auto & endpoint : flight_info.endpoint())
        tickets.push_back(endpoint.ticket().ticket());
}


bool ArrowFlightSource::nextEndpoint()
{
    if (current_endpoint >= tickets.size())
        return false;

    arrow::flight::protocol::Ticket ticket;
    ticket.set_ticket(tickets[current_endpoint]);

    auto stub = connection->getStub(request_timeout_sec);
    auto new_call_context = std::make_shared<grpc::ClientContext>();
    connection->prepareCall(*new_call_context, request_timeout_sec);
    {
        std::lock_guard lock{call_mutex};
        call_context = new_call_context;
    }

    /// ISource::cancel sets is_cancelled before calling onCancel, so a cancellation raised before the call was
    /// published found nothing to abort.
    if (isCancelled())
        new_call_context->TryCancel();

    auto reader = stub->DoGet(new_call_context.get(), ticket);

    /// The first message, the schema, is read here, so that a stream that cannot start is reported as such.
    auto first = std::make_shared<arrow::flight::protocol::FlightData>();
    bool has_first = false;
    while (reader->Read(first.get()))
    {
        if (!first->data_header().empty())
        {
            has_first = true;
            break;
        }
    }
    if (!has_first)
    {
        auto status = reader->Finish();
        if (!status.ok() && !isCancelled())
            throw Exception(
                ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE,
                "Failed to initialize Arrow Flight stream: {}",
                ArrowFlightConnection::formatError(status, *new_call_context));
        if (!status.ok())
            return false;
        /// An empty stream: there is nothing to read from this endpoint.
        {
            std::lock_guard lock{call_mutex};
            call_context = nullptr;
        }
        ++current_endpoint;
        return nextEndpoint();
    }

    auto * reader_ptr = reader.get();
    auto * call_ptr = new_call_context.get();
    startReading([reader_ptr, call_ptr, first, this](ArrowIPC::IPCMessage & message) mutable
    {
        if (first)
        {
            message.header = std::move(*first->mutable_data_header());
            message.body = std::move(*first->mutable_data_body());
            first.reset();
            return true;
        }
        arrow::flight::protocol::FlightData data;
        while (reader_ptr->Read(&data))
        {
            /// A message with metadata only carries no data.
            if (data.data_header().empty())
                continue;
            message.header = std::move(*data.mutable_data_header());
            message.body = std::move(*data.mutable_data_body());
            return true;
        }
        auto status = reader_ptr->Finish();
        stream->finished = true;
        /// The source cancels the call when the query is cancelled, so a cancelled query reaches this as a failed
        /// read. Reporting it would turn `KILL QUERY` into an Arrow transport error.
        if (!status.ok() && !isCancelled())
        {
            const int code = status.error_code() == grpc::StatusCode::UNAVAILABLE ? ErrorCodes::ARROWFLIGHT_CONNECTION_FAILURE
                                                                                   : ErrorCodes::ARROWFLIGHT_INTERNAL_ERROR;
            throw Exception(code, "Arrow Flight internal error: {}", ArrowFlightConnection::formatError(status, *call_ptr));
        }
        return false;
    });
    stream->call_context = new_call_context;
    stream->reader = std::move(reader);

    ++current_endpoint;
    return true;
}

void ArrowFlightSource::onCancel() noexcept
{
    /// Runs in parallel with generate, which blocks inside the read. The mutex may therefore only cover the
    /// handover, never the call itself, and the copy keeps the context alive for the duration of TryCancel.
    std::shared_ptr<grpc::ClientContext> context_to_cancel;
    {
        std::lock_guard lock{call_mutex};
        context_to_cancel = call_context;
    }

    if (context_to_cancel)
        context_to_cancel->TryCancel();
}

Block ArrowFlightSource::fillVirtualColumns(Block result_block)
{
    if (!virtual_header.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual columns: '{}'", virtual_header.getNames());

    return result_block;
}

Chunk ArrowFlightSource::generate()
{
    while (true)
    {
        if (isCancelled())
            return {};

        if (!stream)
        {
            /// Reading the messages of `DoPut` has no endpoints; that stream was the only one.
            if (!connection || !nextEndpoint())
                return {};
        }

        chassert(stream);

        Block block;
        bool has_block = false;
        try
        {
            has_block = stream->executor->pull(block);
        }
        catch (...)
        {
            if (isCancelled())
                return {};
            throw;
        }

        if (!has_block)
        {
            /// We've finished reading from this stream, now it's time to try the next endpoint.
            {
                std::lock_guard lock{call_mutex};
                call_context = nullptr;
            }
            stream.reset();
            if (!connection)
                return {};
            continue;
        }

        if (block.rows() == 0)
            continue;

        auto block_initial = sample_block.cloneWithColumns(block.getColumns());
        auto block_with_virtuals = fillVirtualColumns(std::move(block_initial));
        auto block_projected = convertBlockToHeader(std::move(block_with_virtuals), getPort().getHeader(), context);

        return Chunk(block_projected.getColumns(), block_projected.rows());
    }
}

}

#endif
