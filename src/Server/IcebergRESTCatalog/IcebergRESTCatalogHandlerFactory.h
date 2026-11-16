#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>
#include <Common/logger_useful.h>

namespace DB
{

class IServer;

class IcebergRESTCatalogHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    IcebergRESTCatalogHandlerFactory(IServer & server_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    const std::string name = "IcebergRESTCatalogHandler-factory";
    LoggerPtr log;
    IServer & server;
    IcebergRESTCatalogWarehouseRegistryPtr warehouses;
};

/// Warehouses live in Keeper, under `iceberg_rest_catalog.zookeeper_path` of the server config.
HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server);

}
