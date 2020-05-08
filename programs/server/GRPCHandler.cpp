#include "GRPCHandler.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeQuery.h>
#include <ext/scope_guard.h>
#include <common/getFQDNOrHostName.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <Common/CurrentThread.h>
#include <IO/copyData.h>
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
std::string ParseGrpcPeer(const grpc::ServerContext& context_) {
    String info = context_.peer();
    return info.substr(info.find(":") + 1);
}

void CallDataQuery::ParseQuery() {
        LOG_TRACE(log, "Process query");
        
        Poco::Net::SocketAddress user_adress(ParseGrpcPeer(gRPCcontext));
        LOG_TRACE(log, "Request adress: " << user_adress.toString());

        std::string user = request.user_info().user();
        std::string password = request.user_info().key();
        std::string quota_key = request.user_info().quota();
        
        context.setProgressCallback([this] (const Progress & value) { return progress.incrementPiecewiseAtomically(value); });

        query_context = context;
        query_scope.emplace(*query_context);
        query_context->setUser(user, password, user_adress, quota_key);
        query_context->setCurrentQueryId(request.query_info().query_id());

        ClientInfo & client_info = context.getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;

        client_info.initial_user = client_info.current_user;
        client_info.initial_query_id = client_info.current_query_id;
        client_info.initial_address = client_info.current_address;
}       

void CallDataQuery::ExecuteQuery() {
        LOG_TRACE(log, "Execute query");
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

            if (!insert_query || !insert_query->data)
                throw Exception("Logical error: query requires data to insert, but it is not INSERT query", 0);

            String format = insert_query->format;
            if (format.empty())
            {
                format = "Values";
            }
            ReadBufferFromMemory data_in(insert_query->data, insert_query->end - insert_query->data);
            auto res_stream = query_context->getInputFormat(format, data_in, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

            auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
            if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
            {
                
                StoragePtr storage = DatabaseCatalog::instance().getTable(table_id);
                auto column_defaults = storage->getColumns().getDefaults();
                if (!column_defaults.empty())
                    res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, column_defaults, *query_context);
            }
            copyData(*res_stream, *io.out);
        }
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
    bool sent = false;
    while (!lazy_format->isFinished() && !exception)
    {
        
        if (auto block = lazy_format->getBlock(query_watch.elapsedMilliseconds()))
        {
            query_watch.restart();
            if (!io.null_format)
            {
                sent = senData(block); 
                break;
            }
        } 
        // interactive_delay in miliseconds
        if (progress_watch.elapsedMilliseconds() >= request.interactive_delay())
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
    out->finalize();
}

bool CallDataQuery::senData(const Block & block) {
    out->setResponse([](const String& buffer) {
                         QueryResponse tmp_response;
                         tmp_response.set_output(buffer);
                         return tmp_response;
                    });
    String format = request.query_info().format();
    if (format.empty()) {
        format = "Pretty";
    }
    auto my_block_out_stream = context.getOutputFormat(format, *out, block);
    my_block_out_stream->write(block);
    my_block_out_stream->flush();
    out->next();
    return true;
}

bool CallDataQuery::sendProgress() {
    //TODO fetch_and for total rows?
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
        //TODO format global?
        String format = request.query_info().format();
        //TODO IF NOT JSON*, TabSeparated*, Pretty*
        if (format.empty()) {
            format = "Pretty";
        }
        auto my_block_out_stream = context.getOutputFormat(format, *out, totals);
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
        String format = request.query_info().format();
        //TODO IF NOT JSON*, TabSeparated*, Pretty*
        if (format.empty()) {
            format = "Pretty";
        }
        auto my_block_out_stream = context.getOutputFormat(format, *out, extremes);
        my_block_out_stream->write(extremes);
        my_block_out_stream->flush();
        out->next();
        return true;
    }
    return false;
}

}
