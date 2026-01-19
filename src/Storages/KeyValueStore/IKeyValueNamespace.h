#pragma once

#include <base/types.h>
#include <memory>

namespace DB
{

class IKeyValueStore;

/**
 * Abstract interface representing a namespace (column family) within a Key-Value store.
 * This is used to logically separate different types of data within the same KV store.
 * For RocksDB, this corresponds to ColumnFamilyHandle.
 * For other KV stores, this might be a database name, table name, or bucket.
 */
class IKeyValueNamespace
{
public:
    virtual ~IKeyValueNamespace() = default;

    /// Get the name of this namespace
    virtual String getName() const = 0;

    /// Get the parent KV store
    virtual IKeyValueStore * getStore() const = 0;
};

using IKeyValueNamespacePtr = std::shared_ptr<IKeyValueNamespace>;

}

