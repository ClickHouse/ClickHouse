#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include "config.h"
#include <Poco/Logger.h>

#if USE_ODBC


namespace DB
{

class Context;

/// This handler establishes connection to database, and retrieves whether schema is allowed.
class SchemaAllowedHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit SchemaAllowedHandler(ContextPtr context_) : WithContext(context_), log(getLogger("SchemaAllowedHandler")) { }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
};

}

#endif
