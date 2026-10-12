#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/Common.h>

#include <mutex>

#include <map>
#include <memory>
#include <vector>

namespace DB
{

/// Support multi-level namespace names.
using IcebergNamespaceName = std::vector<String>;

/// Storage backend of the native Iceberg REST catalog (RFC: issue #114697), backed by Keeper.
/// Work in progress, not ready for production use.
/// Layout under `root_path` (one root per warehouse):
///   <root>                                 data: format marker
///   <root>/namespaces/<level>              data: JSON object of namespace properties
///   <root>/namespaces/<level>/namespaces   child namespaces, same shape recursively
///   <root>/namespaces/<level>/tables       reserved for table pointers
/// Namespace levels are encoded with `escapeForFileName`.
class KeeperIcebergRESTCatalogStore
{
public:
    KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_);

    /// Returns false if the namespace already exists. Any other failure throws.
    bool createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties);

    bool namespaceExists(const IcebergNamespaceName & name) const;

    /// Lists direct children of `parent` or top-level namespaces when `parent` is empty.
    std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const;

private:
    /// Returns the current session. Runs `initRoot` once per new session.
    zkutil::ZooKeeperPtr getZooKeeper() const;
    /// Creates the root nodes and checks the format marker.
    void initRoot(const zkutil::ZooKeeperPtr & zookeeper) const;

    String namespacePath(const IcebergNamespaceName & name) const;
    /// Node that holds the direct children of `name`. Empty `name` means the root list.
    String childNamespacesPath(const IcebergNamespaceName & name) const;

    const String root_path;
    const zkutil::GetZooKeeper get_zookeeper;

    /// The session that passed `initRoot`. Requests wait here while a new session is initialized.
    mutable std::mutex mutex;
    mutable zkutil::ZooKeeperPtr initialized_zookeeper TSA_GUARDED_BY(mutex);
};

using KeeperIcebergRESTCatalogStorePtr = std::shared_ptr<KeeperIcebergRESTCatalogStore>;

}
