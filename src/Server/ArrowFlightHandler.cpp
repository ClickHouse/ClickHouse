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
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/Chain.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Access/Credentials.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_CONVERT_TYPE;
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

void ArrowFlightHandler::start() {
    setThreadName("ArrowFlight");

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::ARROW_FLIGHT);
    session->authenticate("default", "", address_to_listen);
    session->makeSessionContext();
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

ArrowFlightHandler::~ArrowFlightHandler() {
   // session.reset();
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
    const arrow::flight::ServerCallContext& arrow_context,
    const arrow::flight::FlightDescriptor& request,
    std::unique_ptr<arrow::flight::PollInfo>* info) {

    (void)arrow_context;
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
    const arrow::flight::ServerCallContext& /*ctx*/,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream)
{
    try
    {
        const std::string sql = request.ticket;

        DB::ThreadStatus thread_status;
        auto query_ctx = session->makeQueryContext();
        query_ctx->setCurrentQueryId("arrow_flight");
        CurrentThread::QueryScope query_scope(query_ctx);
        setThreadName("ArrowFlight");

        DB::QueryFlags flags;
        auto [_, io] = DB::executeQuery(sql, query_ctx, flags, DB::QueryProcessingStage::Complete);
        if (!io.pipeline.pulling())
        {
            return arrow::Status::ExecutionError("DoGet failed: pipeline is not in pulling state");
        }
        auto executor = std::make_unique<DB::PullingPipelineExecutor>(io.pipeline);

        const DB::Block header = executor->getHeader();

        DB::CHColumnToArrowColumn::Settings arrow_settings;
        arrow_settings.output_string_as_string = true;

        DB::CHColumnToArrowColumn converter(header, "Arrow", arrow_settings);

        std::vector<DB::Chunk> chunks;
        DB::Block block;

        while (executor->pull(block))
        {
            chunks.emplace_back(DB::Chunk(block.getColumns(), block.rows()));
        }

        std::shared_ptr<arrow::Table> arrow_table;
        converter.chChunkToArrowTable(arrow_table, chunks, header.columns());

        auto maybe_combined = arrow_table->CombineChunks();
        if (!maybe_combined.ok())
        {
            return arrow::Status::IOError("DoGet failed: cannot combine chunks: " + maybe_combined.status().ToString());
        }

        auto combined_table = maybe_combined.ValueOrDie();
        arrow::TableBatchReader reader(combined_table);

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<arrow::RecordBatch> batch;

        while (true)
        {
            auto st = reader.ReadNext(&batch);
            if (!st.ok())
                return arrow::Status::IOError("DoGet failed while reading batch: " + st.ToString());

            if (!batch)
                break;

            batches.emplace_back(std::move(batch));
        }

        if (batches.empty())
            return arrow::Status::Invalid("DoGet failed: no data produced");

        auto schema = batches.front()->schema();
        auto reader_result = arrow::RecordBatchReader::Make(batches, schema);
        if (!reader_result.ok())
        {
            return arrow::Status::IOError("DoGet failed: " + reader_result.status().ToString());
        }

        *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader_result.ValueOrDie());
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
    std::unique_ptr<arrow::flight::FlightMetadataWriter> /*writer*/)
{
    try
    {
        DB::ThreadStatus thread_status; 
        auto query_context = session->makeQueryContext();
        query_context->setCurrentQueryId("arrow_flight");
        CurrentThread::QueryScope query_scope(query_context);
        setThreadName("ArrowFlight");


        auto schema_result = reader->GetSchema();
        if (!schema_result.ok()) {
            return arrow::Status::Invalid("Failed to receive schema: " + schema_result.status().ToString());
        }
        auto schema = schema_result.ValueOrDie();

        const auto & descriptor = reader->descriptor();
        if (descriptor.type != arrow::flight::FlightDescriptor::PATH || descriptor.path.empty())
        {
            return arrow::Status::IOError("DoPut failed: Invalid descriptor");
        }
        auto dataset_name = descriptor.path[0];
        const auto & table_id = StorageID(query_context->getCurrentDatabase(), dataset_name);
        auto table = DatabaseCatalog::instance().getTable(table_id, query_context);
        auto metadata_snapshot = table->getInMemoryMetadataPtr();
        auto sink = table->write({}, metadata_snapshot, query_context, false);

        // Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "Arrow", false, true, false);
        Block header = metadata_snapshot->getSampleBlock();
        
        ArrowColumnToCHColumn converter(header, "Arrow", true, true, FormatSettings::DateTimeOverflowBehavior::Throw, false, false);


        while (true)
        {
            auto status = reader->Next();
            if (!status.ok())
                return arrow::Status::IOError("Failed to read batch: " + status.status().ToString());
            auto batch = std::move(status.ValueOrDie().data);
            if (!batch)
                break;
            auto batch_result = arrow::Table::FromRecordBatches(schema, {batch});
            if (!batch_result.ok())
            {
                return arrow::Status::IOError("Failed to read batch: " + batch_result.status().ToString());
            }
            auto arrow_table = batch_result.ValueOrDie();
            auto chunk = converter.arrowTableToCHChunk(arrow_table, batch->num_rows());

            auto insert_context = Context::createCopy(query_context);

            auto insert = std::make_shared<ASTInsertQuery>();
            insert->table_id = table_id;
            
            insert->columns = std::make_shared<ASTExpressionList>();
            const auto & columns = metadata_snapshot->getColumns().getOrdinary();
            for (const auto & column : columns)
                insert->columns->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

            

            InterpreterInsertQuery interpreter(
                insert,
                insert_context,
                /* allow_materialized */ true,
                /* no_squash */ false,
                /* no_destination */ false,
                /* async_isnert */ false);
            auto io = interpreter.execute();
            auto input = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));

            io.pipeline.complete(Pipe(std::move(input)));

            CompletedPipelineExecutor executor(io.pipeline);
            executor.execute();

        }

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
