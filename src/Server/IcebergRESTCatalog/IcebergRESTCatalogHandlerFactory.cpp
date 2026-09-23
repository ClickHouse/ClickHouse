#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Common/Exception.h>
#include <Common/escapeForFileName.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

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

HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, const Poco::Util::AbstractConfiguration & config)
{
    auto get_required = [&](const char * key)
    {
        auto value = config.getString(fmt::format("iceberg_rest_catalog.{}", key), "");
        if (value.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'iceberg_rest_catalog.{}' is not set", key);
        return value;
    };
    const auto warehouse = get_required("warehouse");
    auto base_location = get_required("base_location");
    const auto zookeeper_path = config.getString("iceberg_rest_catalog.zookeeper_path", "/clickhouse/iceberg_rest_catalog");

    if (!server.context()->hasZooKeeper())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "The catalog state is stored in Keeper, but no <zookeeper> section is configured");

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
