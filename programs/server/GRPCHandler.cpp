#include "GRPCHandler.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/executeQuery.h>
#include <ext/scope_guard.h>
#include <common/getFQDNOrHostName.h>


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
        
        CurrentThread::QueryScope query_scope(context);

        Poco::Net::SocketAddress user_adress(ParseGrpcPeer(gRPCcontext));
        LOG_TRACE(log, "Request adress: " << user_adress.toString());

        std::string user = request.user_info().user();
        std::string password = request.user_info().key();
        std::string quota_key = request.user_info().quota();

        context.setUser(user, password, user_adress, quota_key);
        context.setCurrentQueryId(request.query_info().query_id());
}

void CallDataQuery::ExecuteQuery() {
        LOG_TRACE(log, "Execute query");
        auto in = std::make_unique<ReadBufferFromString>(request.query_info().query());
        auto used_output = std::make_unique<WriteBufferFromString>(resultQuery);
        executeQuery(*in, *used_output, /* allow_into_outfile = */ false, context, 
        [this] (const String & current_query_id, const String & content_type, const String & format, const String & timezone)
            {
            }
        );
        used_output->finalize();
 }

 }