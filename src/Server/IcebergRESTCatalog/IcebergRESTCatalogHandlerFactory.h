#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/IcebergRESTCatalog/IIcebergRESTCatalogStore.h>
#include <Common/logger_useful.h>

namespace DB
{

class IServer;

class IcebergRESTCatalogHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    IcebergRESTCatalogHandlerFactory(IServer & server_, String warehouse_, IcebergRESTCatalogStorePtr store_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    const std::string name = "IcebergRESTCatalogHandler-factory";
    LoggerPtr log;
    IServer & server;
    const String warehouse;
    IcebergRESTCatalogStorePtr store;
};

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, String warehouse);

}
