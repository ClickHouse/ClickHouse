#include "GRPCHandler.h"
 #include <IO/ReadBufferFromString.h>
#include<IO/WriteBufferFromOStream.h>
 #include <IO/ReadHelpers.h>

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
using GRPCConnection::HelloRequest;
using GRPCConnection::HelloResponse;
using GRPCConnection::QueryRequest;
using GRPCConnection::QueryResponse;
using GRPCConnection::GRPC;


 namespace DB
 {

 std::string ParseGrpcPeer(const grpc::ServerContext& context_) {
    String info = context_.peer();
    return info.substr(info.find(":") + 1);
 }

 static std::chrono::steady_clock::duration parseSessionTimeout(
     const Poco::Util::AbstractConfiguration & config,
     const QueryRequest & request)
 {
     unsigned session_timeout = config.getInt("default_session_timeout", 60);

     if (!request.session_timeout().empty())
     {
         unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
         std::string session_timeout_str = request.session_timeout();

         ReadBufferFromString buf(session_timeout_str);
         if (!tryReadIntText(session_timeout, buf) || !buf.eof())
             throw Exception("Invalid session timeout: '" + session_timeout_str + "'", 0);

         if (session_timeout > max_session_timeout)
             throw Exception("Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + std::to_string(max_session_timeout)
                 + ". Maximum session timeout could be modified in configuration file.",
                 0);
     }

     return std::chrono::seconds(session_timeout);
 }

 void CallDataHello::Proceed(bool ok) {
    if (status == CREATE) {
        status = PROCESS;
        Service->RequestSayHello(&gRPCcontext, &request, &responder, CompilationQueue, CompilationQueue, this);
    } else if (status == PROCESS) {
        LOG_TRACE(log, "Process Hello");
        new CallDataHello(Service, CompilationQueue, iServer, log);
        response.set_response("CLikckHouse " + request.username());
        status = FINISH;
        responder.Finish(response, grpc::Status::OK, this);
    } else {
        GPR_ASSERT(status == FINISH);
        delete this;
    }
 }
 void CallDataQuery::Execute() {


 }
 void CallDataQuery::Proceed(bool ok) {
    if (status == CREATE) {
        status = PREPARE;
        Service->RequestQuery(&gRPCcontext, &request, &responder, CompilationQueue, CompilationQueue, this);
    } else if (status == PREPARE) {
        if(!new_responder_created)
        {
            new CallDataQuery(Service, CompilationQueue, iServer, log);
            new_responder_created = true ;
        }

        LOG_TRACE(log, "Process query");

        String server_display_name = iServer->config().getString("display_name", getFQDNOrHostName());

        CurrentThread::QueryScope query_scope(context);

        Poco::Net::SocketAddress user_adress(ParseGrpcPeer(gRPCcontext));
        LOG_TRACE(log, "Request adress: " << user_adress.toString());

        std::string user = request.x_clickhouse_user();
        std::string password = request.x_clickhouse_key();
        std::string quota_key = request.x_clickhouse_quota();
        if (user.empty() && password.empty() && quota_key.empty())
        {
            user =  "default";
            password = "";
            quota_key = "";
        }

        context.setUser(user, password, user_adress, quota_key);
        context.setCurrentQueryId(request.query_id());

        const auto & config = iServer->config();
        std::shared_ptr<NamedSession> session;
        String session_id;
        std::chrono::steady_clock::duration session_timeout;
        if (!request.session_id().empty())
        {
            session_id = request.session_id();
            session_timeout = parseSessionTimeout(config, request);

            session = context.acquireNamedSession(session_id, session_timeout, true);

            context = session->context;
            context.setSessionContext(session->context);
        }

        SCOPE_EXIT({
            if (session)
                session->release();
        });
        
        
        const char * begin = request.query().data();
        const char * end = begin + request.query().size();
        const Settings & settings = context.getSettingsRef();
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
        io = executeQuery(query, context, false, QueryProcessingStage::Complete, true, true);
        if (io.out) {
            auto in_str = std::make_unique<ReadBufferFromString>(request.query());
            InputStreamFromASTInsertQuery in(ast, in_str.get(), io.out->getHeader(), context, nullptr);
            copyData(in, *io.out);
            LOG_TRACE(log, "Insertion OK");
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
            status = PROCESS;
            response.set_progress("Processing");
            responder.Write(response, (void*)this);
        } else {
            io.onFinish();
            status = ONFINISH;
            response.set_query_id("Done");
            responder.Write(response, (void*)this);
        }
        
    } else if (status == PROCESS){
        SCOPE_EXIT(
                lazy_format->finish();

                try
                {
                    pool.wait();
                }
                catch (...)
                {
                    /// If exception was thrown during pipeline execution, skip it while processing other exception.
                    tryLogCurrentException(log);
                }
        );

        while (!lazy_format->isFinished() && !exception)
        {
            if (auto block = lazy_format->getBlock(progress_watch.elapsedMilliseconds()))
            {
                progress_watch.restart();
                if (!io.null_format) {
                    // std::shared_ptr<WriteBuffer> used_output;
                    
                    // auto in = std::make_unique<ReadBufferFromString>(request.query());
                    // std::ostream& ostr_ = std::clog;
                    // std::vector<char> v;

                    // WriteBufferFromVector used_output(v);
                    // BlockOutputStreamPtr out = std::make_shared<NativeBlockOutputStream>(used_output, 0, block.cloneEmpty(), true);
                    
                    // out->write(block);
                    // out->flush();
                    // // used_output.next();
                    // used_output.finalize();
                    // // String out1(used_output->begin(), used_output->begin()+ used_output->offset() );
                    // for (auto& el : v) {
                    //     LOG_TRACE(log, "LOG:" << el);
                    // }

                    String out1;
                    auto used_output = std::make_unique<WriteBufferFromString>(out1);
                    auto my_block_out_stream = context.getOutputFormat("Pretty", *used_output, block);
                    my_block_out_stream->write(block);
                    my_block_out_stream->flush();
                    LOG_TRACE(log, "LOG:" << out1);
                    response.set_query_id("Result: " + out1);

                }

                    
            }
        }

        lazy_format->finish();
        pool.wait();
        io.onFinish();
        status = ONFINISH;
        // response.set_query_id("Result: ");
        responder.Write(response, (void*)this);

    } else if (status == ONFINISH) {
        status = FINISH;
        responder.Finish(grpc::Status(), (void*)this);
    } else {
        GPR_ASSERT(status == FINISH);
        delete this;
    }
 }

 }