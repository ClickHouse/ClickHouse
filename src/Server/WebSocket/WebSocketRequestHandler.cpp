#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SettingsChanges.h>
#include <Compression/CompressedReadBuffer.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Parsers/ASTSetQuery.h>



namespace DB
{

void WebSocketRequestHandler::processQuery(
    Poco::JSON::Object::Ptr & request,
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
    Poco::JSON::Object::Ptr params;
    if (request->has("params"))
        params = request->get("params").extract<Poco::JSON::Object::Ptr>();

    std::string query_id = "";
    if (request->has("query_id")) {
        query_id = request->get("query_id").extract<std::string>();
    }

    ///temporary solution should be changed cause
    /// The user could hold one session in one webSocket connection and maybe between them.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
    auto client_info = session->getClientInfo();
    auto context = session->makeQueryContext(std::move(client_info));
    const auto & settings = context->getSettingsRef();
    output.setSendProgress(settings.send_progress_in_http_headers);
    output.setSendProgressInterval(settings.http_headers_progress_interval_ms);

    auto param_could_be_skipped = [&] (const String & name)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

        return false;
    };

    std::string database = "";
    std::string default_format = "";


    SettingsChanges settings_changes;
    if (!params.isNull())
    {
        for (const auto & [key, value] : *params)
        {
            if (key == "database")
            {
                if (database.empty())
                    database = value.toString();
            }
            else if (key == "default_format")
            {
                if (default_format.empty())
                    default_format = value.toString();
            }
            else if (param_could_be_skipped(key))
            {
            }
            else
            {
                /// Other than query parameters are treated as settings.
                if (!customizeQueryParam(context, key, value))
                    settings_changes.push_back({key, value.toString()});
            }
        }
    }

    if (!database.empty())
        context->setCurrentDatabase(database);

    if (!default_format.empty())
        context->setDefaultFormat(default_format);

    context->checkSettingsConstraints(settings_changes);
    context->applySettingsChanges(settings_changes);

    context->setCurrentQueryId(query_id);

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



    ReadBufferFromOwnString input(request->get("data").toString());

    executeQuery(input, output, /* allow_into_outfile = */ false, context,
                 [&output] (const QueryResultDetails & details)
                 {
                     output.setQueryId(details.query_id);
                 }
                 );
}

void WebSocketRequestHandler::handleRequest(Poco::JSON::Object::Ptr & request, DB::WebSocket & webSocket)
{
    //auto data = request->get("data").extract<std::string>();


    WriteBufferFromWebSocket output(webSocket, "");
//    std::string str;
//    WriteBufferFromOwnString output;

    std::optional<CurrentThread::QueryScope> query_scope;

    processQuery(request, output, query_scope);


    //webSocket.sendFrame(output.str().c_str(), static_cast<int>(output.str().size()), WebSocket::FRAME_TEXT);

}

    bool WebSocketRequestHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
    {

        if (startsWith(key, QUERY_PARAMETER_NAME_PREFIX))
        {
            /// Save name and values of substitution in dictionary.
            const String parameter_name = key.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

            if (!context->getQueryParameters().contains(parameter_name))
                context->setQueryParameter(parameter_name, value);
            return true;
        }

        return false;
}

}
