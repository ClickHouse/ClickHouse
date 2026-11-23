#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogService.h>
#include <Common/logger_useful.h>

namespace DB
{

class IServer;

class IcebergRESTCatalogHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    IcebergRESTCatalogHandlerFactory(IServer & server_, IcebergRESTCatalogServicePtr service_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    const std::string name = "IcebergRESTCatalogHandler-factory";
    LoggerPtr log;
    IServer & server;
    IcebergRESTCatalogServicePtr service;
};

/// Warehouses live in Keeper, under `iceberg_rest_catalog.zookeeper_path` of the server config.
HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server);

}
