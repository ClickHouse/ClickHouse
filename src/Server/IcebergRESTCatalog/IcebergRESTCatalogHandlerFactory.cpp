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

IcebergRESTCatalogHandlerFactory::IcebergRESTCatalogHandlerFactory(IServer & server_, IcebergRESTCatalogWarehousesPtr warehouses_)
    : log(getLogger(name))
    , server(server_)
    , warehouses(std::move(warehouses_))
{
}

std::unique_ptr<HTTPRequestHandler> IcebergRESTCatalogHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP request for {}. {}", name, request.toStringForLogging());
    return std::make_unique<IcebergRESTCatalogHandler>(server, warehouses);
}

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, String warehouse, String base_location, const String & zookeeper_path)
{
    auto root_path = std::filesystem::path(zookeeper_path) / escapeForFileName(warehouse);
    auto store = std::make_shared<KeeperIcebergRESTCatalogStore>(
        [context = server.context()] { return context->getZooKeeper(); }, root_path.string());

    IcebergRESTCatalogWarehouses::Map warehouses;
    warehouses.emplace(
        warehouse,
        std::make_shared<const IcebergRESTCatalogWarehouse>(IcebergRESTCatalogWarehouse{
            .name = warehouse,
            .base_location = std::move(base_location),
            .store = std::move(store),
        }));
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(
        server, std::make_shared<const IcebergRESTCatalogWarehouses>(std::move(warehouses)));
}

}
