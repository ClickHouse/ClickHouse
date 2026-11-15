#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Response with 404 and verbose description.
class KeeperNotFoundHandler : public HTTPRequestHandler
{
public:
    explicit KeeperNotFoundHandler(std::vector<std::string> hints_) : hints(std::move(hints_)) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    std::vector<std::string> hints;
};

}
