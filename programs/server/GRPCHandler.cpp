#include "GRPCHandler.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeQuery.h>
#include <ext/scope_guard.h>
#include <common/getFQDNOrHostName.h>
#include <Common/SettingsChanges.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <Common/CurrentThread.h>
#include <IO/copyData.h>
#include <IO/ConcatReadBuffer.h>
#include <DataStreams/copyData.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <Storages/IStorage.h>

using GRPCConnection::QueryRequest;
using GRPCConnection::QueryResponse;
using GRPCConnection::GRPC;


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int NO_DATA_TO_INSERT;
}

std::string ParseGrpcPeer(const grpc::ServerContext& context_) {
    String info = context_.peer();
    return info.substr(info.find(":") + 1);
}

void CallDataQuery::ParseQuery() {
    LOG_TRACE(log, "Process query");
    
    Poco::Net::SocketAddress user_adress(ParseGrpcPeer(gRPCcontext));
    LOG_TRACE(log, "Request: " << request.query_info().query());

    std::string user = request.user_info().user();
    std::string password = request.user_info().password();
    std::string quota_key = request.user_info().quota();
    interactive_delay = request.interactive_delay();
    if (interactive_delay == 0)
        interactive_delay = INT_MAX;
    context.setProgressCallback([this] (const Progress & value) { return progress.incrementPiecewiseAtomically(value); });


    query_context = context;
    query_scope.emplace(*query_context);
    query_context->setUser(user, password, user_adress, quota_key);
    query_context->setCurrentQueryId(request.query_info().query_id());

    SettingsChanges settings_changes;
    for (const auto & [key, value] : request.query_info().settings())
    {
        if (key == "database")
        {
            if (!DatabaseCatalog::instance().isDatabaseExist(value))
            {
                Exception e("Database " + value + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            }
            query_context->setCurrentDatabase(value);
        }
        else if (key == "default_format")
        {
            format_output = value;
            query_context->setDefaultFormat(value);
        }
        else
        {
            settings_changes.push_back({key, value});
        }
    }

    query_context->checkSettingsConstraints(settings_changes);
    query_context->applySettingsChanges(settings_changes);

    ClientInfo & client_info = query_context->getClientInfo();
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.interface = ClientInfo::Interface::GRPC;

    client_info.initial_user = client_info.current_user;
    client_info.initial_query_id = client_info.current_query_id;
    client_info.initial_address = client_info.current_address;
}       

void CallDataQuery::ParseData() {
    LOG_TRACE(log, "ParseData");
    const char * begin = request.query_info().query().data();
    const char * end = begin + request.query_info().query().size();
    const Settings & settings = query_context->getSettingsRef();
    
    ParserQuery parser(end, settings.enable_debug_queries);
    ASTPtr ast = parseQuery(parser, begin, end, "", settings.max_query_size, settings.max_parser_depth);
    
    auto * insert_query = ast->as<ASTInsertQuery>();
    auto query_end = end;

    if (insert_query && insert_query->data)
    {
        query_end = insert_query->data;
    }
    String query(begin, query_end);
    io = executeQuery(query, *query_context, false, QueryProcessingStage::Complete, true, true);
    if (io.out) {

        if (!insert_query || !(insert_query->data || request.query_info().insert_data() || !request.insert_data().empty()))
        {
            Exception e("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::NO_DATA_TO_INSERT);
        }

        format_input = insert_query->format;
        if (format_input.empty())
            format_input = "Values";

        if (format_output.empty())
            format_output = format_input;
        ConcatReadBuffer::ReadBuffers buffers;
        std::shared_ptr<ReadBufferFromMemory> data_in_query;
        std::shared_ptr<ReadBufferFromMemory> data_in_insert_data;
        if (insert_query->data)
        {
            data_in_query = std::make_shared<ReadBufferFromMemory>(insert_query->data, insert_query->end - insert_query->data);
            buffers.push_back(data_in_query.get());
        }

        if (!request.insert_data().empty())
        {
            data_in_insert_data = std::make_shared<ReadBufferFromMemory>(request.insert_data().data(), request.insert_data().size());
            buffers.push_back(data_in_insert_data.get());
        }
        auto input_buffer_contacenated = std::make_unique<ConcatReadBuffer>(buffers);
        auto res_stream = query_context->getInputFormat(format_input, *input_buffer_contacenated, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

        auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
        if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
        {
            
            StoragePtr storage = DatabaseCatalog::instance().getTable(table_id);
            auto column_defaults = storage->getColumns().getDefaults();
            if (!column_defaults.empty())
                res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, column_defaults, *query_context);
        }
        io.out->writePrefix();
        while(auto block = res_stream->read()) {
            io.out->write(block);
        }
        if (request.query_info().insert_data()) {
            status = READ_DATA;
            responder.Read(&request, (void*)this);
            return;
        }
        io.out->writeSuffix();
    }

    ExecuteQuery();
}
void CallDataQuery::ReadData() {
    LOG_TRACE(log, "Read data" << request.insert_data() << " dsdc");
    if (request.insert_data().empty()) {
        io.out->writeSuffix();
        ExecuteQuery();
    } else {
        const char * begin = request.insert_data().data();
        const char * end = begin + request.insert_data().size();
        ReadBufferFromMemory data_in(begin, end - begin);
        auto res_stream = query_context->getInputFormat(format_input, data_in, io.out->getHeader(), query_context->getSettings().max_insert_block_size);
        
        while(auto block = res_stream->read())
            io.out->write(block);
        responder.Read(&request, (void*)this);
    }
}
void CallDataQuery::ExecuteQuery(){
    LOG_TRACE(log, "ExecuteQuery");
    if (io.pipeline.initialized()) {
        auto & header = io.pipeline.getHeader();
        auto thread_group = CurrentThread::getGroup();

        lazy_format = std::make_shared<LazyOutputFormat>(io.pipeline.getHeader());
        io.pipeline.setOutput(lazy_format);
        executor = io.pipeline.execute();

        pool.scheduleOrThrowOnError([&]()
        {
            try
            {
                executor->execute(io.pipeline.getNumThreads());
            }
            catch (...)
            {
                exception = true;
                throw;
            }
        });
        query_watch.start();
        progress_watch.start();
        ProgressQuery();
    } else {
        FinishQuery();
    }
}

void CallDataQuery::ProgressQuery() {
    LOG_TRACE(log, "Proccess");
    status = PROGRESS;
    bool sent = false;
    while (!lazy_format->isFinished() && !exception)
    {
        
        if (auto block = lazy_format->getBlock(query_watch.elapsedMilliseconds()))
        {
            query_watch.restart();
            if (!io.null_format)
            {
                sent = sendData(block); 
                break;
            }
        } 
        // interactive_delay in miliseconds
        if (progress_watch.elapsedMilliseconds() >= interactive_delay)
        {
            progress_watch.restart();
            sent = sendProgress(); 
            break;
        }
    }
    if ((lazy_format->isFinished() || exception) && !sent)
    {
        lazy_format->finish();
        pool.wait();
        SendDetails();
    }
}
void CallDataQuery::SendDetails() {
    bool sent = false;
    while (!sent) {
        switch( detailsStatus ) {
            case SEND_TOTALS:
            {
                sent = sendTotals(lazy_format->getTotals());
                detailsStatus = SEND_EXTREMES;
                break;
            }
            case SEND_EXTREMES:
            {
                sent = sendExtremes(lazy_format->getExtremes());
                detailsStatus = FINISH;
                break;
            }
            case FINISH:
            {
                sent = true;
                FinishQuery();
            }
        }
    }
}

void CallDataQuery::FinishQuery() {
    io.onFinish();
    query_scope->logPeakMemoryUsage();
    status = FINISH_QUERY;
    out->finalize();
}

bool CallDataQuery::sendData(const Block & block) {
    out->setResponse([](const String& buffer) {
                         QueryResponse tmp_response;
                         tmp_response.set_output(buffer);
                         return tmp_response;
                    });
    auto my_block_out_stream = query_context->getOutputFormat(format_output, *out, block);
    my_block_out_stream->write(block);
    my_block_out_stream->flush();
    out->next();
    return true;
}

bool CallDataQuery::sendProgress() {
    auto grpcProgress = [](const String& buffer) {
        auto in = std::make_unique<ReadBufferFromString>(buffer);
        ProgressValues progressValues;
        progressValues.read(*in, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO);
        GRPCConnection::Progress progress;
        progress.set_read_rows(progressValues.read_rows);
        progress.set_read_bytes(progressValues.read_bytes);
        progress.set_total_rows_to_read(progressValues.total_rows_to_read);
        progress.set_written_rows(progressValues.written_rows);
        progress.set_written_bytes(progressValues.written_bytes);
        return progress;
    };

    out->setResponse([&grpcProgress](const String& buffer) {
                        QueryResponse tmp_response;
                        auto progress = std::make_unique<GRPCConnection::Progress>(grpcProgress(buffer));
                        tmp_response.set_allocated_progress(progress.release());
                        return tmp_response;
                    });
    auto increment = progress.fetchAndResetPiecewiseAtomically();
    increment.write(*out, DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO);
    out->next();
    return true;
}

bool CallDataQuery::sendTotals(const Block & totals) {
    if (totals) {
        out->setResponse([](const String& buffer) {
                         QueryResponse tmp_response;
                         tmp_response.set_totals(buffer);
                         return tmp_response;
                    });
        //TODO IF NOT JSON*, TabSeparated*, Pretty*
        auto my_block_out_stream = query_context->getOutputFormat(format_output, *out, totals);
        my_block_out_stream->write(totals);
        my_block_out_stream->flush();
        out->next();
        return true;
    }
    return false;
}

bool CallDataQuery::sendExtremes(const Block & extremes) {
    if (extremes) {
        out->setResponse([](const String& buffer) {
                         QueryResponse tmp_response;
                         tmp_response.set_extremes(buffer);
                         return tmp_response;
                    });
        //TODO IF NOT JSON*, TabSeparated*, Pretty*
        auto my_block_out_stream = context.getOutputFormat(format_output, *out, extremes);
        my_block_out_stream->write(extremes);
        my_block_out_stream->flush();
        out->next();
        return true;
    }
    return false;
}

}
