#pragma once

#include <Storages/KeyValueStore/IKeyValueIterator.h>
#include <Storages/KeyValueStore/IKeyValueNamespace.h>
#include <base/types.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace DB
{

/**
 * Status codes for KV store operations.
 * This abstracts away backend-specific error codes.
 */
enum class KVStatus
{
    OK = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IOError = 5,
    MergeInProgress = 6,
    Incomplete = 7,
    ShutdownInProgress = 8,
    TimedOut = 9,
    Aborted = 10,
    Busy = 11,
    Expired = 12,
    TryAgain = 13,
    Unknown = 999
};

/**
 * Batch operation for batchWrite.
 * Each operation specifies its namespace, operation type, key, and optional value.
 */
struct BatchOperation
{
    IKeyValueNamespacePtr namespace_ptr;
    enum Type { DELETE, PUT };
    Type type;
    String key;
    String value;  // Only used for PUT operations

    BatchOperation(IKeyValueNamespacePtr ns, Type t, const String & k, const String & v = "")
        : namespace_ptr(ns), type(t), key(k), value(v) {}
};

/**
 * Abstract interface for Key-Value store operations.
 * This provides a generic interface that can be implemented by different
 * KV store backends (RocksDB, LevelDB, in-memory stores, etc.)
 */
class IKeyValueStore
{
public:
    virtual ~IKeyValueStore() = default;

    /// Create or get a namespace by name
    virtual IKeyValueNamespacePtr getOrCreateNamespace(const String & name) = 0;

    /// Get an existing namespace by name (returns nullptr if not found)
    virtual IKeyValueNamespacePtr getNamespace(const String & name) const = 0;

    /// Drop a namespace (if supported)
    virtual KVStatus dropNamespace(const String & name) = 0;

    /// List all namespace names
    virtual std::vector<String> listNamespaces() const = 0;

    /// Put a key-value pair into the specified namespace
    virtual KVStatus put(IKeyValueNamespace * ns, const String & key, const String & value) = 0;

    /// Get a value by key from the specified namespace
    virtual KVStatus get(IKeyValueNamespace * ns, const String & key, String & value) const = 0;

    /// Delete a key from the specified namespace
    virtual KVStatus del(IKeyValueNamespace * ns, const String & key) = 0;

    /// Delete a range of keys [start_key, end_key) from the specified namespace
    virtual KVStatus delRange(IKeyValueNamespace * ns, const String & start_key, const String & end_key) = 0;

    /// Get all key-value pairs with a given prefix from the specified namespace
    virtual void getByPrefix(
        IKeyValueNamespace * ns,
        const String & prefix,
        std::vector<String> & keys,
        std::vector<String> & values) const = 0;

    /// Get all values with a given prefix from the specified namespace
    virtual void getValuesByPrefix(
        IKeyValueNamespace * ns,
        const String & prefix,
        std::vector<String> & values) const = 0;

    /// Get all values in a key range [begin_key, end_key) from the specified namespace
    virtual void getValuesByRange(
        IKeyValueNamespace * ns,
        const String & begin_key,
        const String & end_key,
        std::vector<String> & values) const = 0;

    /// Create a new iterator for the specified namespace
    virtual IKeyValueIteratorPtr createIterator(IKeyValueNamespace * ns) const = 0;

    /// Execute a batch of operations atomically
    virtual KVStatus batchWrite(const std::vector<BatchOperation> & operations) = 0;

    /// Get an estimated number of keys in the specified namespace
    virtual uint64_t getEstimateNumKeys(IKeyValueNamespace * ns) const = 0;

    /// Get a property value (backend-specific, e.g., "rocksdb.cur-size-all-mem-tables")
    virtual std::unordered_map<String, size_t> getProperty(IKeyValueNamespace * ns, const String & property) const = 0;

    /// Flush all namespaces to disk
    virtual void flush() = 0;

    /// Shutdown the KV store
    virtual void shutdown() = 0;

    /// Get raw backend handle (for advanced usage, may return nullptr)
    virtual void * getRawHandle() const { return nullptr; }
};

using IKeyValueStorePtr = std::shared_ptr<IKeyValueStore>;

}

