#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Common/logger_useful.h>

namespace DB
{

class IServer;

class IcebergRESTCatalogHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    IcebergRESTCatalogHandlerFactory(IServer & server_, String warehouse_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    const std::string name = "IcebergRESTCatalogHandler-factory";
    LoggerPtr log;
    IServer & server;
    const String warehouse;
    const String base_location;
    KeeperIcebergRESTCatalogStorePtr store;
};

/// The catalog state lives in Keeper under `<zookeeper_path>/<warehouse>`.
/// `base_location` is the storage prefix for tables created without an explicit location.
HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, String warehouse, String base_location, const String & zookeeper_path);

}
