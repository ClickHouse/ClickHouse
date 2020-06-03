#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPRequestHandler.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Response with custom string. Can be used for browser.
class StaticRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;

    int status;
    String content_type;
    String response_expression;

public:
    StaticRequestHandler(IServer & server, const String & expression, int status_ = 200, const String & content_type_ = "text/html; charset=UTF-8");

    void writeResponse(WriteBuffer & out);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;
};

}
