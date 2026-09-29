#pragma once

#include <base/types.h>

#include <map>
#include <memory>
#include <vector>

namespace DB
{

/// Support multi-level namespace names.
using IcebergNamespaceName = std::vector<String>;

/// Storage backend of native Iceberg REST catalog (RFC: issue #114697).
/// Work in progress, not ready for production use.
class IIcebergRESTCatalogStore
{
public:
    virtual ~IIcebergRESTCatalogStore() = default;

    /// Returns false if the namespace already exists. Any other failure throws.
    virtual bool createNamespace(const IcebergNamespaceName & name, std::map<String, String> properties) = 0;

    virtual bool namespaceExists(const IcebergNamespaceName & name) const = 0;

    /// Lists direct children of `parent` or top-level namespaces when `parent` is empty.
    virtual std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const = 0;
};

using IcebergRESTCatalogStorePtr = std::shared_ptr<IIcebergRESTCatalogStore>;

}
