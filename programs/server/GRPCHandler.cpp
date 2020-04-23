#include "GRPCHandler.h"
 #include <IO/ReadBufferFromString.h>
 #include <IO/ReadHelpers.h>

 #include <Interpreters/executeQuery.h>
 #include <ext/scope_guard.h>
#include <common/getFQDNOrHostName.h>


 using GRPCConnection::HelloRequest;
 using GRPCConnection::HelloResponse;
 using GRPCConnection::QueryRequest;
 using GRPCConnection::QueryResponse;
 using GRPCConnection::GRPC;


 namespace DB
 {

 namespace ErrorCodes
 {
     extern const int READONLY;
     extern const int UNKNOWN_COMPRESSION_METHOD;

     extern const int CANNOT_PARSE_TEXT;
     extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
     extern const int CANNOT_PARSE_QUOTED_STRING;
     extern const int CANNOT_PARSE_DATE;
     extern const int CANNOT_PARSE_DATETIME;
     extern const int CANNOT_PARSE_NUMBER;
     extern const int CANNOT_OPEN_FILE;

     extern const int UNKNOWN_ELEMENT_IN_AST;
     extern const int UNKNOWN_TYPE_OF_AST_NODE;
     extern const int TOO_DEEP_AST;
     extern const int TOO_BIG_AST;
     extern const int UNEXPECTED_AST_STRUCTURE;

     extern const int SYNTAX_ERROR;

     extern const int INCORRECT_DATA;
     extern const int TYPE_MISMATCH;

     extern const int UNKNOWN_TABLE;
     extern const int UNKNOWN_FUNCTION;
     extern const int UNKNOWN_IDENTIFIER;
     extern const int UNKNOWN_TYPE;
     extern const int UNKNOWN_STORAGE;
     extern const int UNKNOWN_DATABASE;
     extern const int UNKNOWN_SETTING;
     extern const int UNKNOWN_DIRECTION_OF_SORTING;
     extern const int UNKNOWN_AGGREGATE_FUNCTION;
     extern const int UNKNOWN_FORMAT;
     extern const int UNKNOWN_DATABASE_ENGINE;
     extern const int UNKNOWN_TYPE_OF_QUERY;

     extern const int QUERY_IS_TOO_LARGE;

     extern const int NOT_IMPLEMENTED;
     extern const int SOCKET_TIMEOUT;

     extern const int UNKNOWN_USER;
     extern const int WRONG_PASSWORD;
     extern const int REQUIRED_PASSWORD;

     extern const int INVALID_SESSION_TIMEOUT;
     extern const int HTTP_LENGTH_REQUIRED;
 }

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
             throw Exception("Invalid session timeout: '" + session_timeout_str + "'", ErrorCodes::INVALID_SESSION_TIMEOUT);

         if (session_timeout > max_session_timeout)
             throw Exception("Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + std::to_string(max_session_timeout)
                 + ". Maximum session timeout could be modified in configuration file.",
                 ErrorCodes::INVALID_SESSION_TIMEOUT);
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
    }
 }

void CallDataQuery::Execute() {
    auto in = std::make_unique<ReadBufferFromString>(request.query());
    try{
        executeQuery(*in, *used_output, /* allow_into_outfile = */ false, context, 
            [this] (const String & current_query_id, const String & content_type, const String & format, const String & timezone)
                {
                }
            );
        progress_query.store(2);
    } catch(...) {
        progress_query.store(2);
        LOG_TRACE(log, "Fault");
    }
}

 void CallDataQuery::Proceed(bool ok) {
    if (status == CREATE) {
        status = PROCESS;
        Service->RequestQuery(&gRPCcontext, &request, &responder, CompilationQueue, CompilationQueue, this);
    } else if (status == PROCESS) {
        if(!new_responder_created)
        {
            new CallDataQuery(Service, CompilationQueue, iServer, log);
            new_responder_created = true ;
        }
        if (progress_query.load() == 0) {
            LOG_TRACE(log, "Process query");
            
            
            used_output = std::make_unique<WriteBufferFromString>(out);

            String server_display_name = iServer.config().getString("display_name", getFQDNOrHostName());

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
            } else if (user.empty() || password.empty()){
                throw Exception("Invalid authentication: required password", ErrorCodes::REQUIRED_PASSWORD);
            }

            context.setUser(user, password, user_adress, quota_key);
            context.setCurrentQueryId(request.query_id());

            const auto & config = iServer.config();
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
            auto appendCallback = [this] (ProgressCallback callback)
            {
                auto prev = context.getProgressCallback();

                context.setProgressCallback([prev, callback] (const Progress & progress)
                {
                    if (prev)
                        prev(progress);

                    callback(progress);
                });
            };
            if (request.interactive_delay() != 0)
                appendCallback([this] (const Progress & progress) {
                    accumulated_progress.incrementPiecewiseAtomically(progress);
                });
            
            std::thread execute1 (&CallDataQuery::Execute, this);
            execute = std::move(execute1);
            int zero = 0;
            progress_query.compare_exchange_strong(zero, 1);
            response.set_progress("Executing" + std::to_string(progress_query.load()));
            responder.Write(response, (void*)this);
            LOG_TRACE(log, "Sent proccess");
            
        } else if (progress_query.load() == 1) {
            while (progress_watch.elapsed() < request.interactive_delay() * 1000000 and progress_query.load() == 1) {
                // LOG_TRACE(log, "Delay: " + std::to_string(request.interactive_delay()));
            }
            LOG_TRACE(log, "Delay: " + std::to_string(request.interactive_delay()));
            progress_watch.restart();
            WriteBufferFromOwnString progress_string_writer;
            accumulated_progress.writeJSON(progress_string_writer);
            response.set_progress(progress_string_writer.str());
            responder.Write(response, (void*)this);   

        } else if (progress_query.load() == 2) {
            execute.join();
            response.set_progress("");
            response.set_query_id("Resut: "+ out);
            responder.Write(response, (void*)this);
            status = FINISH;
            responder.Finish(grpc::Status(), (void*)this);
        }
    } else {
        GPR_ASSERT(status == FINISH);
    }
 }

 }