#pragma once

#include <Storages/MergeTree/Manifest/IManifestStorage.h>

namespace DB
{

/**
 * Factory for creating manifest storage instances.
 * 
 * Currently supports:
 * - "rocksdb" - RocksDB implementation (requires USE_ROCKSDB)
 * 
 * Future: "memory" for testing, "leveldb", etc.
 */
class ManifestStorageFactory
{
public:
    /**
     * Create a manifest storage instance
     * @param type_str - type string ("rocksdb", etc.)
     * @param dir - directory path for the storage
     * @return manifest storage instance
     * @throws Exception if type is not supported or creation fails
     */
    static ManifestStoragePtr create(
        const String & type_str,
        const String & dir);

    /**
     * Check if a manifest storage type is supported
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
