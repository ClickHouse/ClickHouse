#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include "config.h"
#include <Poco/Logger.h>

#if USE_ODBC

/// This handler establishes connection to database, and retrieve quote style identifier
namespace DB
{

class IdentifierQuoteHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit IdentifierQuoteHandler(ContextPtr context_) : WithContext(context_), log(getLogger("IdentifierQuoteHandler")) { }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
};

}

#endif
