#pragma once
#include <Common/config.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>

#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
/** This handler establish connection to database, and retrieve quote style identifier
  */
namespace DB
{
class IdentifierQuoteHandler : public Poco::Net::HTTPRequestHandler
{
public:
    IdentifierQuoteHandler(size_t keep_alive_timeout_, std::shared_ptr<Context> context_)
        : log(&Poco::Logger::get("IdentifierQuoteHandler")), keep_alive_timeout(keep_alive_timeout_), context(context_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
    std::shared_ptr<Context> context;
};
}
#endif
