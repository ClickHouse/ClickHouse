#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Common/escapeForFileName.h>

#include <filesystem>

namespace DB
{

IcebergRESTCatalogHandlerFactory::IcebergRESTCatalogHandlerFactory(IServer & server_, String warehouse_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_)
    : log(getLogger(name))
    , server(server_)
    , warehouse(std::move(warehouse_))
    , base_location(std::move(base_location_))
    , store(std::move(store_))
{
}

std::unique_ptr<HTTPRequestHandler> IcebergRESTCatalogHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP request for {}. {}", name, request.toStringForLogging());
    return std::make_unique<IcebergRESTCatalogHandler>(server, warehouse, base_location, store);
}

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, String warehouse, String base_location, const String & zookeeper_path)
{
    auto root_path = std::filesystem::path(zookeeper_path) / escapeForFileName(warehouse);
    auto store = std::make_shared<KeeperIcebergRESTCatalogStore>(
        [context = server.context()] { return context->getZooKeeper(); }, root_path.string());
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(server, std::move(warehouse), std::move(base_location), std::move(store));
}

}
