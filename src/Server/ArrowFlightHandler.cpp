#include "ArrowFlightHandler.h"

#include <memory>
#include <arrow/result.h>
#include <arrow/flight/api.h>
#include <arrow/flight/types.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>
#include <arrow/compute/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <arrow/util/macros.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/IServer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

ArrowFlightHandler::ArrowFlightHandler(IServer & server_, const Poco::Net::SocketAddress & address_to_listen_)
    : server(server_)
    , log(getLogger("ArrowFlightHandler"))
    , address_to_listen(address_to_listen_)
{
    auto parse_location_status = arrow::flight::Location::ForGrpcTcp(address_to_listen_.host().toString(), address_to_listen_.port());
    if (!parse_location_status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Invalid address {} for Arrow Flight Server: {}",
                        address_to_listen_.toString(),
                        parse_location_status->ToString());
    }
    location = std::move(parse_location_status).ValueOrDie();
}

ArrowFlightHandler::~ArrowFlightHandler() {}

void ArrowFlightHandler::start() {
    setThreadName("ArrowFlight");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::ARROW_FLIGHT);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(last_connection_id++);

    arrow::flight::FlightServerOptions options(location);
    auto init_status = Init(options);
    if (!init_status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed init Arrow Flight Server: {}",
                        init_status.ToString());
    }

    auto serve_status = Serve();
    if (!serve_status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed serve Arrow Flight: {}",
                        serve_status.ToString());
    }
}

void ArrowFlightHandler::stop() {
    auto status = Shutdown();
    if (!status.ok()) {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed shutdown Arrow Flight: {}",
                        status.ToString());
    }
}

UInt16 ArrowFlightHandler::portNumber() const {
    return address_to_listen.port();
}

arrow::Status ArrowFlightHandler::ListFlights(
    const arrow::flight::ServerCallContext&, const arrow::flight::Criteria*,
    std::unique_ptr<arrow::flight::FlightListing>* listings) {
    std::vector<arrow::flight::FlightInfo> flights;
    *listings = std::make_unique<arrow::flight::SimpleFlightListing>(std::move(flights));
    return arrow::Status::OK();
}

arrow::Status ArrowFlightHandler::GetFlightInfo(
    const arrow::flight::ServerCallContext& /*context*/,
    const arrow::flight::FlightDescriptor& /*request*/,
    std::unique_ptr<arrow::flight::FlightInfo>* /*info*/) {
    return arrow::Status::OK();
}

arrow::Status ArrowFlightHandler::PollFlightInfo(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::FlightDescriptor& request,
    std::unique_ptr<arrow::flight::PollInfo>* info) {

    (void)context;
    (void)request;
    (void)info;
    return arrow::Status::OK();
}

arrow::Status ArrowFlightHandler::GetSchema(
    const arrow::flight::ServerCallContext& /*context*/,
    const arrow::flight::FlightDescriptor& request,
    std::unique_ptr<arrow::flight::SchemaResult>* schema) {

    String query = request.cmd;
    ContextMutablePtr query_context = Context::createCopy(server.context());
    ReadBufferFromString input_buffer(query);
    WriteBufferFromOwnString output_buffer;

    try
    {
        executeQuery(input_buffer, output_buffer, false, query_context, {});
        *schema = std::make_unique<arrow::flight::SchemaResult>("");
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::IOError("GetSchema failed: " + e.displayText());
    }
}

arrow::Status ArrowFlightHandler::DoGet(
    const arrow::flight::ServerCallContext& /*context*/,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {

    String query = std::string(reinterpret_cast<const char *>(request.ticket.data()), request.ticket.size());
    ContextMutablePtr query_context = Context::createCopy(server.context());
    ReadBufferFromString input_buffer(query);
    WriteBufferFromOwnString output_buffer;

    try
    {
        BlockIO io;
        executeQuery(input_buffer, output_buffer, false, query_context, {});

        auto pipeline = std::move(io.pipeline);
        auto executor = std::make_unique<PullingPipelineExecutor>(pipeline);

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<arrow::Schema> arrow_schema;
        arrow::ipc::DictionaryMemo dict_memo;

        Block block;
        // bool first = true;

        while (executor->pull(block))
        {
            // if (first)
            // {
            //     auto schema_result = getArrowSchema(block, "Arrow", query_context->getSettingsRef());
            //     arrow_schema = schema_result->schema;
            //     dict_memo = schema_result->dictionary_memo;
            //     first = false;
            // }

            // auto batch = blockToArrowRecordBatch(block, arrow_schema, query_context->getSettingsRef(), &dict_memo);
            // batches.push_back(batch);
        }

        auto maybe_table = arrow::Table::FromRecordBatches(arrow_schema, batches);
        if (!maybe_table.ok())
            return arrow::Status::ExecutionError(maybe_table.status().ToString());

        std::shared_ptr<arrow::Table> table = *maybe_table;
        auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
        *stream = std::make_unique<arrow::flight::RecordBatchStream>(batch_reader);
        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::IOError("DoGet failed: " + e.displayText());
    }
}

arrow::Status ArrowFlightHandler::DoPut(
    const arrow::flight::ServerCallContext& /*context*/,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> /*writer*/) {

    try
    {
        ContextMutablePtr query_context = Context::createCopy(server.context());
        auto schema_result = reader->GetSchema();
        if (!schema_result.ok()) {
            return arrow::Status::Invalid("Failed to receive schema: " + schema_result.status().ToString());
        }
        auto schema = schema_result.ValueOrDie();

        auto arrow_table_result = reader->ToTable();
        if (!arrow_table_result.ok()) {
            return arrow::Status::Invalid("Failed to receive table: " + arrow_table_result.status().ToString());
        }

        auto arrow_table = arrow_table_result.ValueOrDie();

        Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "Arrow", false, true, false);
        ArrowColumnToCHColumn converter(header, "Arrow", true, true, FormatSettings::DateTimeOverflowBehavior::Throw, false, false);

        Chunk chunk = converter.arrowTableToCHChunk(arrow_table, arrow_table->num_rows());

        /// TODO: Здесь необходимо записать chunk в конкретную таблицу.

        return arrow::Status::OK();
    }
    catch (const DB::Exception & e)
    {
        return arrow::Status::ExecutionError("DoPut failed: " + e.displayText());
    }
}

arrow::Status ArrowFlightHandler::DoExchange(
    const arrow::flight::ServerCallContext& /*context*/,
    std::unique_ptr<arrow::flight::FlightMessageReader> /*reader*/,
    std::unique_ptr<arrow::flight::FlightMessageWriter> /*writer*/) {
    return arrow::Status::NotImplemented("DoExchange is not implemented");
}

arrow::Status ArrowFlightHandler::DoAction(
    const arrow::flight::ServerCallContext& /*context*/,
    const arrow::flight::Action& /*action*/,
    std::unique_ptr<arrow::flight::ResultStream>* /*result*/) {
    return arrow::Status::OK();
}

arrow::Status ArrowFlightHandler::ListActions(
    const arrow::flight::ServerCallContext& /*context*/,
    std::vector<arrow::flight::ActionType>* /*actions*/) {
    return arrow::Status::OK();
}

} // namespace DB
