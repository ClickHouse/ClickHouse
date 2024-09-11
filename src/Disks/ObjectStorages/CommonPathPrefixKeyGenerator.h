#pragma once

#include <Common/ObjectStorageKeyGenerator.h>

#include <filesystem>
#include <map>
#include <optional>

namespace DB
{

/// Deprecated. Used for backward compatibility with plain rewritable disks without a separate metadata layout.
/// Object storage key generator used specifically with the
/// MetadataStorageFromPlainObjectStorage if multiple writes are allowed.

/// It searches for the local (metadata) path in a pre-loaded path map.
/// If no such path exists, it searches for the parent path, until it is found
/// or no parent path exists.
///
/// The key generator ensures that the original directory hierarchy is
/// preserved, which is required for the MergeTree family.

struct InMemoryPathMap;
class CommonPathPrefixKeyGenerator : public IObjectStorageKeysGenerator
{
public:
    /// Local to remote path map. Leverages filesystem::path comparator for paths.

    explicit CommonPathPrefixKeyGenerator(String key_prefix_, std::weak_ptr<InMemoryPathMap> path_map_);

    ObjectStorageKey generate(const String & path, bool is_directory, const std::optional<String> & key_prefix) const override;

private:
    /// Longest key prefix and unresolved parts of the source path.
    std::tuple<std::string, std::vector<String>> getLongestObjectKeyPrefix(const String & path) const;

    const String storage_key_prefix;

    std::weak_ptr<InMemoryPathMap> path_map;
};

}
