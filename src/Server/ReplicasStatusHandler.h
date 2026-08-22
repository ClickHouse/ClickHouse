#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{

class Context;
class IServer;

/// Replies "Ok.\n" if all replicas on this server don't lag too much. Otherwise output lag information.
class ReplicasStatusHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit ReplicasStatusHandler(IServer & server_);
    explicit ReplicasStatusHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
        : ReplicasStatusHandler(server_)
    {
        http_response_headers_override = http_response_headers_override_;
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    /// Overrides for response headers.
    std::unordered_map<String, String> http_response_headers_override;
};


}
