#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandlerFactory.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperPathUtils.h>
#include <Common/escapeForFileName.h>
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

ObjectStoragePtr createWarehouseObjectStorage([[maybe_unused]] IServer & server, [[maybe_unused]] const String & storage_named_collection)
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
    const auto zookeeper_path_with_name = config.getString("iceberg_rest_catalog.zookeeper_path", "/clickhouse/iceberg_rest_catalog");

    /// The path may select an auxiliary Keeper with a `name:/path` prefix, like other Keeper path settings.
    const auto zookeeper_name = zkutil::extractZooKeeperName(zookeeper_path_with_name);
    const auto zookeeper_path = zkutil::extractZooKeeperPath(zookeeper_path_with_name, /*check_starts_with_slash*/ true);

    if (zookeeper_name == zkutil::DEFAULT_ZOOKEEPER_NAME)
    {
        if (!server.context()->hasZooKeeper())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "The catalog state is stored in Keeper, but no <zookeeper> section is configured");
    }
    else if (!server.context()->hasAuxiliaryZooKeeper(zookeeper_name))
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "The catalog state is stored in auxiliary Keeper '{}', but it is not configured in <auxiliary_zookeepers>",
            zookeeper_name);
    }

    auto root_path = std::filesystem::path(zookeeper_path) / escapeForFileName(warehouse_name);
    auto keeper_store = std::make_shared<KeeperIcebergRESTCatalogStore>(
        [context = server.context(), zookeeper_name] { return context->getDefaultOrAuxiliaryZooKeeper(zookeeper_name); },
        root_path.string());

    auto object_storage = createWarehouseObjectStorage(server, storage_named_collection);

    auto warehouse_ptr = std::make_shared<const IcebergRESTCatalogWarehouse>(
        warehouse_name, base_location, std::move(keeper_store), std::move(object_storage));

    IcebergRESTCatalogWarehouses::Map warehouses;
    warehouses.emplace(warehouse_name, std::move(warehouse_ptr));
    return std::make_shared<IcebergRESTCatalogHandlerFactory>(
        server, std::make_shared<const IcebergRESTCatalogWarehouses>(std::move(warehouses)));
}

}
