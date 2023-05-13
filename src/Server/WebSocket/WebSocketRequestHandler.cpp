#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressedReadBuffer.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>

namespace DB
{


void WebSocketRequestHandler::processQuery(
    Poco::JSON::Object & request,
    WriteBufferFromWebSocket & output,
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
    auto client_info = session->getClientInfo();
    auto context = session->makeQueryContext(std::move(client_info));

    context->setCurrentQueryId("");

    query_scope.emplace(context);

    //const auto & settings = context->getSettingsRef();

    auto append_callback = [context = context] (ProgressCallback callback)
    {
        auto prev = context->getProgressCallback();

        context->setProgressCallback([prev, callback] (const Progress & progress)
                                     {
                                         if (prev)
                                             prev(progress);

                                         callback(progress);
                                     });
    };

    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
    /// Note that we add it unconditionally so the progress is available for `X-ClickHouse-Summary`
    append_callback([&output](const Progress & progress) { output.onProgress(progress); });

//    if (settings.readonly > 0 && settings.cancel_http_readonly_queries_on_client_close)
//    {
//        append_callback([&context, &request](const Progress &)
//                        {
//                            /// Assume that at the point this method is called no one is reading data from the socket any more:
//                            /// should be true for read-only queries.
//                            /// TODO: Make some check
////                            if (false)
////                                context->killCurrentQuery();
//                        });
//    }

    ReadBufferFromOwnString input(request.get("data").toString());

    executeQuery(input, output, /* allow_into_outfile = */ false, context, nullptr);
}

void WebSocketRequestHandler::handleRequest(Poco::JSON::Object & request, DB::WebSocket & webSocket)
{
    auto data = request.get("data").extract<std::string>();
    WriteBufferFromWebSocket output(webSocket, true);
//    std::string str;
//    WriteBufferFromOwnString output;

    std::optional<CurrentThread::QueryScope> query_scope;

    processQuery(request, output, query_scope);


    //webSocket.sendFrame(output.str().c_str(), static_cast<int>(output.str().size()), WebSocket::FRAME_TEXT);

}

}
