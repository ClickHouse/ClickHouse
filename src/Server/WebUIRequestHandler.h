#pragma once

#include <Poco/Net/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with HTML page that allows to send queries and show results in browser.
class WebUIRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;
    std::string resource_name;
public:
    WebUIRequestHandler(IServer & server_, std::string resource_name_);
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;
};

}

