#pragma once

#if defined(OS_LINUX)

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IServer.h>


namespace DB
{

/// Handles WebSocket connections for the web terminal interface.
/// On regular GET requests, serves the HTML page.
/// On WebSocket upgrade requests, establishes a pseudoterminal session
/// with the embedded clickhouse-client.
class WebTerminalRequestHandler : public HTTPRequestHandler
{
public:
    explicit WebTerminalRequestHandler(IServer & server_) : server(server_) {}

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    IServer & server;

    /// Serve the static HTML page for the web terminal UI.
    void serveHTML(HTTPServerRequest & request, HTTPServerResponse & response);

    /// Handle a WebSocket upgrade request: authenticate, create PTY, bridge data.
    void handleWebSocket(HTTPServerRequest & request, HTTPServerResponse & response);
};

}

#endif
