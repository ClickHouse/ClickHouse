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
        query_context = context;
        query_scope.emplace(*query_context);
        query_context->setUser(user, password, user_adress, quota_key);
        query_context->setCurrentQueryId(request.query_info().query_id());
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
            LOG_TRACE(log, "Insertion" << insert_query->data);
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
            // BlockInputStreamPtr block_input = context.getInputFormat(
            // current_format, buf, sample, insert_format_max_block_size);
            // // auto in_str = std::make_unique<ReadBufferFromString>(query_tail);
            // InputStreamFromASTInsertQuery in(ast, nullptr, io.out->getHeader(), context, nullptr);
            
            auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
            if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
            {
                
                StoragePtr storage = DatabaseCatalog::instance().getTable(table_id);
                auto column_defaults = storage->getColumns().getDefaults();
                if (!column_defaults.empty())
                    LOG_TRACE(log, "Insertion IF" << table_id);
                    res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, column_defaults, *query_context);
            }
            copyData(*res_stream, *io.out);
        }
        if (io.pipeline.initialized()) {
            auto & header = io.pipeline.getHeader();
            size_t num_threads = 1;
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
        if (auto block = lazy_format->getBlock(progress_watch.elapsedMilliseconds()))
        {
            progress_watch.restart();
            if (!io.null_format)
            {
                String out1;
                auto used_output = std::make_unique<WriteBufferFromString>(out1);
                auto my_block_out_stream = context.getOutputFormat("Pretty", *out, block);
                my_block_out_stream->write(block);
                my_block_out_stream->flush();
                LOG_TRACE(log, "LOG:" << out1);

                out->next();
                sent = out->isWritten();
                
                LOG_TRACE(log, "Wrote");
                break;
            }
        }
    }
    if ((lazy_format->isFinished() || exception) && !sent) {

        FinishQuery();
    }
    LOG_TRACE(log, "Wrote");
}

void CallDataQuery::FinishQuery() {
    LOG_TRACE(log, "Finish");
    if (lazy_format) {
        lazy_format->finish();
        pool.wait();
    }
    io.onFinish();
    query_scope->logPeakMemoryUsage();
    out->finalize();
}
}