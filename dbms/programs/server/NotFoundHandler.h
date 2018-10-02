#pragma once

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

/// Response with 404 and verbose description.
class NotFoundHandler : public Poco::Net::HTTPRequestHandler
{
public:
    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
