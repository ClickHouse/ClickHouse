#pragma once

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with 404 and verbose description.
class NotFoundHandler : public Poco::Net::HTTPRequestHandler
{
public:
    NotFoundHandler(const std::string & no_handler_description_) : no_handler_description(no_handler_description_) {}

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    const std::string no_handler_description;
};

}
