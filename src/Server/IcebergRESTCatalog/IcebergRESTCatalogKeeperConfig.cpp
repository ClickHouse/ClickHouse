#include <Server/IcebergRESTCatalog/IcebergRESTCatalogKeeperConfig.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperPathUtils.h>
#include <Common/escapeForFileName.h>
#include <Interpreters/Context.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

String IcebergRESTCatalogKeeperConfig::warehousePath(const String & warehouse_name) const
{
    return (std::filesystem::path(zookeeper_path) / escapeForFileName(warehouse_name)).string();
}

zkutil::ZooKeeperPtr IcebergRESTCatalogKeeperConfig::getZooKeeper(const Context & context) const
{
    return context.getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
}

IcebergRESTCatalogKeeperConfig getIcebergRESTCatalogKeeperConfig(const Context & context)
{
    const auto zookeeper_path_with_name
        = context.getConfigRef().getString("iceberg_rest_catalog.zookeeper_path", "/clickhouse/iceberg_rest_catalog");

    /// The path may select an auxiliary Keeper with a `name:/path` prefix, like other Keeper path settings.
    IcebergRESTCatalogKeeperConfig result;
    result.zookeeper_name = zkutil::extractZooKeeperName(zookeeper_path_with_name);
    result.zookeeper_path = zkutil::extractZooKeeperPath(zookeeper_path_with_name, /*check_starts_with_slash*/ true);

    if (result.zookeeper_name == zkutil::DEFAULT_ZOOKEEPER_NAME)
    {
        if (!context.hasZooKeeper())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "The catalog state is stored in Keeper, but no <zookeeper> section is configured");
    }
    else if (!context.hasAuxiliaryZooKeeper(result.zookeeper_name))
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "The catalog state is stored in auxiliary Keeper '{}', but it is not configured in <auxiliary_zookeepers>",
            result.zookeeper_name);
    }
    return result;
}

}
