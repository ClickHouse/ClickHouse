#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Response with 404 and verbose description.
class NotFoundHandler : public HTTPRequestHandler
{
public:
    explicit NotFoundHandler(std::vector<std::string> hints_) : hints(std::move(hints_)) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    std::vector<std::string> hints;
};

}
