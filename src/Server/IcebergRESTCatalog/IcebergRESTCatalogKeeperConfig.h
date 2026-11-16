#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/Common.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Keeper settings from the `iceberg_rest_catalog` section of the server config.
struct IcebergRESTCatalogKeeperConfig
{
    /// `default`, or the auxiliary Keeper named in `zookeeper_path` as `<name>:/path`.
    String zookeeper_name;
    /// Parent node of all warehouse nodes.
    String zookeeper_path;

    String warehousePath(const String & warehouse_name) const;

    /// The Keeper session that holds the catalog state.
    zkutil::ZooKeeperPtr getZooKeeper(const Context & context) const;
};

IcebergRESTCatalogKeeperConfig getIcebergRESTCatalogKeeperConfig(const Context & context);

}
