#pragma once

#include <Storages/KeyValueStore/IKeyValueStore.h>
#include <vector>
#include <string>

namespace DB
{

/**
 * Factory for creating KV store instances.
 * 
 * Supports different KV store types:
 * - "rocksdb" - RocksDB implementation (requires USE_ROCKSDB)
 * - Future: "memory", "leveldb", etc.
 */
class KeyValueStoreFactory
{
public:
    /**
     * Create a KV store instance
     * @param type_str - type string ("rocksdb", etc.)
     * @param dir - directory path for the KV store
     * @param expect_namespaces - expected namespace names (will be created if not exist)
     * @return KV store instance
     * @throws Exception if type is not supported or creation fails
     */
    static IKeyValueStorePtr create(
        const String & type_str,
        const String & dir,
        const std::vector<std::string> & expect_namespaces = {});

    /**
     * Check if a KV store type is supported
     * @param type_str - type string
     * @return true if supported
     */
    static bool isTypeSupported(const String & type_str);

    /**
     * Get list of supported types
     * @return list of type strings
     */
    static std::vector<String> getSupportedTypes();
};

} // namespace DB

