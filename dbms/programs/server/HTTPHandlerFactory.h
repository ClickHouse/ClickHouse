#pragma once

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>
#include "IServer.h"
#include "InterserverIOHTTPHandler.h"


namespace DB
{

class InterserverIOHTTPHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    InterserverIOHTTPHandlerFactory(IServer & server_, const std::string & name_);

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override;

private:
    IServer & server;
    Logger * log;
    std::string name;
};

using HTTPHandlerCreator = std::function<Poco::Net::HTTPRequestHandler * ()>;
using HTTPHandlerMatcher = std::function<bool(const Poco::Net::HTTPServerRequest &)>;
using HTTPHandlerMatcherAndCreator = std::pair<HTTPHandlerMatcher, HTTPHandlerCreator>;
using HTTPHandlersMatcherAndCreator = std::vector<HTTPHandlerMatcherAndCreator>;

class HTTPHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    HTTPHandlerFactory(IServer & server_, const std::string & name_);

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override;

    void updateHTTPHandlersCreator(Poco::Util::AbstractConfiguration & configuration, const String & key = "http_handlers");

private:
    IServer & server;
    Logger * log;
    std::string name;

    String no_handler_description;
    HTTPHandlersMatcherAndCreator handlers_creator;
};

}
