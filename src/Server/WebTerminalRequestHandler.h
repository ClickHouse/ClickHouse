#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IServer.h>

#include <base/types.h>

#include <optional>


namespace DB
{

/// Handles WebSocket connections for the web terminal interface.
/// On regular GET requests, serves the HTML page.
/// On WebSocket upgrade requests, establishes a pseudoterminal session
/// with the embedded clickhouse-client.
class WebTerminalRequestHandler : public HTTPRequestHandler
{
public:
    explicit WebTerminalRequestHandler(IServer & server_, std::optional<String> default_session_user_ = {})
        : server(server_), default_session_user(std::move(default_session_user_)) {}

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    IServer & server;

    /// If set, overrides the `default_session_user` server setting for this endpoint
    /// (composable protocols allow a per-endpoint default user). An empty value prohibits
    /// anonymous connections (a WebSocket auth message without a "user" field).
    std::optional<String> default_session_user;

    /// Serve the static HTML page for the web terminal UI.
    void serveHTML(HTTPServerRequest & request, HTTPServerResponse & response);

    /// Handle a WebSocket upgrade request: authenticate, create PTY, bridge data.
    void handleWebSocket(HTTPServerRequest & request, HTTPServerResponse & response);
};

}
