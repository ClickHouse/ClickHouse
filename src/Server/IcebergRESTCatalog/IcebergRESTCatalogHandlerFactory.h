#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>
#include <Common/logger_useful.h>

namespace DB
{

class IServer;

class IcebergRESTCatalogHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    IcebergRESTCatalogHandlerFactory(IServer & server_, IcebergRESTCatalogWarehousesPtr warehouses_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    const std::string name = "IcebergRESTCatalogHandler-factory";
    LoggerPtr log;
    IServer & server;
    IcebergRESTCatalogWarehousesPtr warehouses;
};

/// The catalog state lives in Keeper under `<zookeeper_path>/<warehouse>`.
/// `base_location` is the storage prefix for tables created without an explicit location.
/// The config defines a single warehouse for now. Warehouses will be stored in Keeper and managed with SQL later.
HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, String warehouse, String base_location, const String & zookeeper_path);

}
