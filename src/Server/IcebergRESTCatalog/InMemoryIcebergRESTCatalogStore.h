#pragma once

#include <Server/IcebergRESTCatalog/IIcebergRESTCatalogStore.h>

#include <mutex>

namespace DB
{

/// Process-local catalog store, lost on server restart. Scaffolding until the Keeper-backed store lands.
class InMemoryIcebergRESTCatalogStore : public IIcebergRESTCatalogStore
{
public:
    bool createNamespace(const IcebergNamespaceName & name, std::map<String, String> properties) override;
    bool namespaceExists(const IcebergNamespaceName & name) const override;
    std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const override;

private:
    mutable std::mutex mutex;
    /// Flat map of full multi-level names; std::map keeps listing deterministic.
    std::map<IcebergNamespaceName, std::map<String, String>> namespaces;
};

/// One store per warehouse, shared across listeners for all listen hosts, STOP/START LISTEN cycles and config reloads.
IcebergRESTCatalogStorePtr getSharedInMemoryIcebergRESTCatalogStore(const String & warehouse);

}
