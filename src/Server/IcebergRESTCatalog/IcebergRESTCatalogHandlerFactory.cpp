#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

#include <Poco/Util/AbstractConfiguration.h>

#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

ObjectStoragePtr createWarehouseObjectStorage(IServer & server, const String & storage_named_collection)
{
#if USE_AWS_S3
    auto configuration = std::make_shared<StorageS3Configuration>();
    ASTs args;
    args.push_back(make_intrusive<ASTIdentifier>(storage_named_collection));
    StorageObjectStorageConfiguration::initialize(*configuration, args, server.context(), /*with_table_structure*/ false);

    return configuration->createObjectStorage(server.context(), /*is_readonly*/ false, std::nullopt);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The Iceberg REST catalog needs S3 support, but this build has none");
#endif
}

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

/// Temporary: builds the single warehouse from the server config. The config is development scaffolding.
/// Warehouses will be created with SQL and stored in Keeper instead.
HTTPRequestHandlerFactoryPtr createIcebergRESTCatalogHandlerFactory(IServer & server, const Poco::Util::AbstractConfiguration & config)
{
    auto get_required = [&](const char * key)
    {
        auto value = config.getString(fmt::format("iceberg_rest_catalog.{}", key), "");
        if (value.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "'iceberg_rest_catalog.{}' is not set", key);
        return value;
    };
    const auto warehouse_name = get_required("warehouse");
    const auto base_location = stripTrailingSlashes(get_required("base_location"));
    const auto storage_named_collection = get_required("storage_named_collection");
    const auto zookeeper_path = config.getString("iceberg_rest_catalog.zookeeper_path", "/clickhouse/iceberg_rest_catalog");

    if (!server.context()->hasZooKeeper())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "The catalog state is stored in Keeper, but no <zookeeper> section is configured");

    auto root_path = std::filesystem::path(zookeeper_path) / escapeForFileName(warehouse_name);
    auto keeper_store = std::make_shared<KeeperIcebergRESTCatalogStore>(
        [context = server.context()] { return context->getZooKeeper(); }, root_path.string());

    auto object_storage = createWarehouseObjectStorage(server, storage_named_collection);

    auto warehouse_ptr = std::make_shared<const IcebergRESTCatalogWarehouse>(
        warehouse_name, base_location, std::move(keeper_store), std::move(object_storage));

    IcebergRESTCatalogWarehouses::Map warehouses;
    warehouses.emplace(warehouse_name, std::move(warehouse_ptr));
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(
        server, std::make_shared<const IcebergRESTCatalogWarehouses>(std::move(warehouses)));
}

}
