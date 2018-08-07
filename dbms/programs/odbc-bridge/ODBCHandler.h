#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Data/SessionPool.h>
#pragma GCC diagnostic pop

namespace DB
{
class ODBCHTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    ODBCHTTPHandler(std::shared_ptr<Poco::Data::SessionPool> pool_,
        const std::string & format_,
        size_t max_block_size_,
        size_t keep_alive_timeout_,
        std::shared_ptr<Context> context_)
        : log(&Poco::Logger::get("ODBCHTTPHandler"))
        , pool(pool_)
        , format(format_)
        , max_block_size(max_block_size_)
        , keep_alive_timeout(keep_alive_timeout_)
        , context(context_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    std::shared_ptr<Poco::Data::SessionPool> pool;
    std::string format;
    size_t max_block_size;
    size_t keep_alive_timeout;
    std::shared_ptr<Context> context;
};

class PingHandler : public Poco::Net::HTTPRequestHandler
{
public:
    PingHandler(size_t keep_alive_timeout_) : keep_alive_timeout(keep_alive_timeout_) {}
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    size_t keep_alive_timeout;
};

class ODBCRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    ODBCRequestHandlerFactory(const std::string & name_, size_t keep_alive_timeout_, std::shared_ptr<Context> context_)
        : log(&Poco::Logger::get(name_)), name(name_), keep_alive_timeout(keep_alive_timeout_), context(context_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override;

private:
    Poco::Logger * log;
    std::string name;
    size_t keep_alive_timeout;
    std::shared_ptr<Context> context;
    std::unordered_map<std::string, std::shared_ptr<Poco::Data::SessionPool>> pool_map;
};
}
