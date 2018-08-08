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
class HandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    HandlerFactory(const std::string & name_, size_t keep_alive_timeout_, std::shared_ptr<Context> context_)
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
