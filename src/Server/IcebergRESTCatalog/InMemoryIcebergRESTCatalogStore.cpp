#include <Server/IcebergRESTCatalog/InMemoryIcebergRESTCatalogStore.h>

#include <algorithm>

namespace DB
{

bool InMemoryIcebergRESTCatalogStore::createNamespace(const IcebergNamespaceName & name, std::map<String, String> properties)
{
    std::lock_guard lock(mutex);
    if (!namespaces.emplace(name, std::move(properties)).second)
        return false;

    /// Create the parents if needed, so that the tree stays walkable from the root.
    for (size_t level = 1; level < name.size(); ++level)
        namespaces.emplace(IcebergNamespaceName(name.begin(), name.begin() + level), std::map<String, String>{});

    return true;
}

bool InMemoryIcebergRESTCatalogStore::namespaceExists(const IcebergNamespaceName & name) const
{
    std::lock_guard lock(mutex);
    return namespaces.contains(name);
}

std::vector<IcebergNamespaceName> InMemoryIcebergRESTCatalogStore::listNamespaces(const IcebergNamespaceName & parent) const
{
    std::lock_guard lock(mutex);
    std::vector<IcebergNamespaceName> result;
    for (const auto & [name, _] : namespaces)
    {
        if (name.size() == parent.size() + 1 && std::equal(parent.begin(), parent.end(), name.begin()))
            result.push_back(name);
    }
    return result;
}

IcebergRESTCatalogStorePtr getSharedInMemoryIcebergRESTCatalogStore(const String & warehouse)
{
    static std::mutex mutex;
    static std::map<String, IcebergRESTCatalogStorePtr> stores;

    std::lock_guard lock(mutex);
    auto & store = stores[warehouse];
    if (!store)
        store = std::make_shared<InMemoryIcebergRESTCatalogStore>();
    return store;
}

}
