#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressedReadBuffer.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>

namespace DB
{


void WebSocketRequestHandler::processQuery(
    Poco::JSON::Object & request,
    WriteBuffer & output,
    std::optional<CurrentThread::QueryScope> & query_scope
)
{
    // TODO: make appropriate changes to make this thing work
//    using namespace Poco::Net;

//    /// The user could specify session identifier and session timeout.
//    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
//     const auto & config = server.config();
//

    ///temporary solution should be changed cause
    /// The user could hold one session in one webSocket connection and maybe between them.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
     auto session_ = std::make_shared<Session>(server.context(), ClientInfo::Interface::WEB_SOCKET, true);

     session_->makeSessionContext();
      auto client_info = session_->getClientInfo();
      auto context = session_->makeQueryContext(std::move(client_info));

      context->setCurrentQueryId("");

      query_scope.emplace(context);


    ReadBufferFromOwnString input(request.get("data").toString());

    executeQuery(input, output, /* allow_into_outfile = */ false, context, nullptr);
}

void WebSocketRequestHandler::handleRequest(Poco::JSON::Object & request, DB::WebSocket & webSocket)
{
    auto data = request.get("data").extract<std::string>();
    WriteBufferFromWebSocket output(webSocket, true);

    std::optional<CurrentThread::QueryScope> query_scope;

    processQuery(request, output, query_scope);
    //webSocket.sendFrame(data.c_str(), std::min(std::max(0,static_cast<int>(data.size())), 10000000), 0x81);

}

}
