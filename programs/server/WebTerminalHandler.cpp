#include "WebTerminalHandler.h"

#include "WebTerminalSession.h"

#include <ctime>
#include <IO/HTTPCommon.h>

#include <ext/scope_guard.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/config_version.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Common/HTMLForm.h>

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <unordered_map>

extern const unsigned char _binary_terminal_html_end[];
extern const unsigned char _binary_terminal_html_start[];
extern const unsigned char _binary_web_terminal_actions_js_end[];
extern const unsigned char _binary_web_terminal_actions_js_start[];
extern const unsigned char _binary_web_terminal_initializer_js_end[];
extern const unsigned char _binary_web_terminal_initializer_js_start[];

namespace DB
{

namespace ErrorCodes
{
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int UNKNOWN_REQUEST_PATH;
}

HTTPWebTerminalHandler::HTTPWebTerminalHandler(IServer & server)
    : context(server.context())
{
    session_timeout = std::chrono::minutes(30);
    /// TODO: session_timeout
}

using ActionFunction = void (WebTerminalSession::*)(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse &, const HTMLForm &);

static inline ActionFunction getActionFunction(const StringRef & action_name)
{
    const static std::unordered_map<std::string, ActionFunction> actions_table
    {
        {"login", &WebTerminalSession::login},
        {"output", &WebTerminalSession::output},
        {"cancel", &WebTerminalSession::cancelQuery},
        {"execute_query", &WebTerminalSession::executeQuery},
        {"configuration", &WebTerminalSession::configuration}
    };

    const auto & iterator = actions_table.find(action_name.toString());

    if (iterator == actions_table.end())
        throw Exception("An unknown resource was requested.", ErrorCodes::UNKNOWN_REQUEST_PATH);

    return iterator->second;
}

static inline void processGetHTTPRequest(const String & basename, Poco::Net::HTTPServerResponse & response)
{
    static const std::unordered_map<std::string, std::tuple<const char *, const unsigned char *, const unsigned char *>> resources_map
    {
        {
            "session",
            {"text/html;charset=utf-8",               _binary_terminal_html_start,               _binary_terminal_html_end}
        },
        {
            "web_terminal_actions.js",
            {"application/javascript; charset=utf-8", _binary_web_terminal_actions_js_start,     _binary_web_terminal_actions_js_end}
        },
        {
            "web_terminal_initializer.js",
            {"application/javascript; charset=utf-8", _binary_web_terminal_initializer_js_start, _binary_web_terminal_initializer_js_end}
        }
    };

    const auto & iterator = resources_map.find(basename);

    if (iterator == resources_map.end())
        throw Exception("An unknown resource was requested.", ErrorCodes::UNKNOWN_REQUEST_PATH);

    response.setContentType(std::get<0>(iterator->second));
    response.sendBuffer(std::get<1>(iterator->second), std::get<2>(iterator->second) - std::get<1>(iterator->second));
}

static inline void trySendExceptionToClient(
    int exception_code, const String & exception_message,
    Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    try
    {
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
            && response.getKeepAlive() && !request.stream().eof()
            && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED)
        {
            request.stream().ignore(std::numeric_limits<std::streamsize>::max());
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_LENGTH_REQUIRED);
        }
        else
        {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        }

        if (!response.sent())
        {
            response.set("X-ClickHouse-Exception-Code", toString(exception_code));
            response.send() << exception_message << std::endl;
        }
    }
    catch (...)
    {
        tryLogCurrentException("HTTPWebTerminalHandler", "Cannot send exception to client");
    }
}

void HTTPWebTerminalHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("WTerminal");
    ThreadStatus thread_status;

    try
    {
        /// For keep-alive to work.
        if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
            throw Exception("The Transfer-Encoding is not chunked and there is no Content-Length header for POST request", ErrorCodes::HTTP_LENGTH_REQUIRED);

        Poco::URI resource_uri(request.getURI());

        std::vector<String> segments;
        resource_uri.getPathSegments(segments);

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET && segments.size() == 1 && segments[0] == "console")
            response.redirect("/console/" + generateRandomString(6) + "/session?" + resource_uri.getRawQuery());
        else if (segments.size() == 3 && segments[0] == "console")
        {
            if (segments[2] == "session")
            {
                const auto & session = getWebTerminalSessions().emplace(segments[1], session_timeout, context);
                session->applySettings(HTMLForm(request));
            }

            if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
                processGetHTTPRequest(segments[2], response);
            else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
            {
                HTMLForm params(request);
                params.load(request, request.stream());
                const auto & session = getWebTerminalSessions().emplace(segments[1], session_timeout, context);
                (session.get()->*getActionFunction(segments[2]))(request, response, params);
            }
            else
                throw Exception("An unknown resource was requested.", ErrorCodes::UNKNOWN_REQUEST_PATH);
        }
        else
            throw Exception("An unknown resource was requested.", ErrorCodes::UNKNOWN_REQUEST_PATH);
    }
    catch (...)
    {
        tryLogCurrentException("HTTPWebTerminalHandler");
        trySendExceptionToClient(getCurrentExceptionCode(), getCurrentExceptionMessage(false, true), request, response);
    }
}

}
