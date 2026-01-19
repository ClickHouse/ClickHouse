#pragma once

#include <Core/Types.h>
#include <base/types.h>
#include <memory>
#include <vector>

namespace DB
{

/**
 * Status codes for manifest storage operations.
 */
enum class ManifestStatus : int32_t
{
    OK = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IOError = 5,
    Busy = 6,
    TimedOut = 7,
};

/**
 * Iterator for iterating over key-value pairs in manifest storage.
 */
class IManifestIterator
{
public:
    virtual ~IManifestIterator() = default;

    /// Seek to the first key >= target_key
    virtual void seek(const String & key) = 0;

    /// Move to the next key-value pair
    virtual void next() = 0;

    /// Check if the iterator is pointing to a valid key-value pair
    virtual bool valid() const = 0;

    /// Get the current key
    virtual String key() const = 0;

    /// Get the current value
    virtual String value() const = 0;
};

using ManifestIteratorPtr = std::unique_ptr<IManifestIterator>;

/**
 * Abstract interface for manifest storage.
 * 
 * Manifest storage is used to store part metadata in a KV store,
 * avoiding filesystem scans when loading parts at startup.
 * 
 * Key format: {uuid} (e.g., "550e8400-e29b-41d4-a716-446655440000")
 * Value format: Serialized PartObject (protobuf)
 */
class IManifestStorage
{
public:
    virtual ~IManifestStorage() = default;

    /// Put a key-value pair
    /// @param key - part UUID as string
    /// @param value - serialized PartObject
    /// @return operation status
    virtual ManifestStatus put(const String & key, const String & value) = 0;

    /// Get a value by key
    /// @param key - part UUID as string
    /// @param value - output parameter for the serialized PartObject
    /// @return operation status (ManifestStatus::NotFound if key doesn't exist)
    virtual ManifestStatus get(const String & key, String & value) const = 0;

    /// Delete a key
    /// @param key - part UUID as string
    /// @return operation status
    virtual ManifestStatus del(const String & key) = 0;

    /// Delete a range of keys [start_key, end_key)
    /// @param start_key - inclusive start key
    /// @param end_key - exclusive end key
    /// @return operation status
    virtual ManifestStatus delRange(const String & start_key, const String & end_key) = 0;

    /// Batch write: delete and put operations in a single transaction
    /// @param keys_to_delete - keys to delete
    /// @param keys_to_put - key-value pairs to put
    /// @return operation status
    virtual ManifestStatus batchWrite(
        const std::vector<String> & keys_to_delete,
        const std::vector<std::pair<String, String>> & keys_to_put) = 0;

    /// Get all key-value pairs with the given prefix
    /// @param prefix - key prefix (empty string means all keys)
    /// @param keys - output: matching keys
    /// @param values - output: corresponding values
    virtual void getByPrefix(
        const String & prefix,
        std::vector<String> & keys,
        std::vector<String> & values) const = 0;

    /// Get all values with the given prefix (keys are not needed)
    /// @param prefix - key prefix (empty string means all values)
    /// @param values - output: matching values
    virtual void getValuesByPrefix(
        const String & prefix,
        std::vector<String> & values) const = 0;

    /// Get all values in a key range [begin_key, end_key)
    /// @param begin_key - inclusive start key
    /// @param end_key - exclusive end key
    /// @param values - output: matching values
    virtual void getValuesByRange(
        const String & begin_key,
        const String & end_key,
        std::vector<String> & values) const = 0;

    /// Create an iterator
    /// @return iterator instance
    virtual ManifestIteratorPtr createIterator() const = 0;

    /// Get estimated number of keys
    /// @return estimated count
    virtual uint64_t getEstimateNumKeys() const = 0;

    /// Flush all pending writes to persistent storage
    virtual void flush() = 0;

    /// Shutdown the manifest storage and release all resources
    virtual void shutdown() = 0;
};

using ManifestStoragePtr = std::shared_ptr<IManifestStorage>;

} // namespace DB
