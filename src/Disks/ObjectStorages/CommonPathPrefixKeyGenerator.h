#pragma once

#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <filesystem>
#include <map>

namespace DB
{

/// Object storage key generator used specifically with the
/// MetadataStorageFromPlainObjectStorage if multiple writes are allowed.

/// It searches for the local (metadata) path in a pre-loaded path map.
/// If no such path exists, it searches for the parent path, until it is found
/// or no parent path exists.
///
/// The key generator ensures that the original directory hierarchy is
/// preserved, which is required for the MergeTree family.
class CommonPathPrefixKeyGenerator : public IObjectStorageKeysGenerator
{
public:
    /// Local to remote path map. Leverages filesystem::path comparator for paths.
    using PathMap = std::map<std::filesystem::path, std::string>;

    explicit CommonPathPrefixKeyGenerator(String key_prefix_, SharedMutex & shared_mutex_, std::weak_ptr<PathMap> path_map_);

    ObjectStorageKey generate(const String & path, bool is_directory) const override;

private:
    /// Longest key prefix and unresolved parts of the source path.
    std::tuple<std::string, std::vector<String>> getLongestObjectKeyPrefix(const String & path) const;

    const String storage_key_prefix;

    SharedMutex & shared_mutex;
    std::weak_ptr<PathMap> path_map;
};

}
