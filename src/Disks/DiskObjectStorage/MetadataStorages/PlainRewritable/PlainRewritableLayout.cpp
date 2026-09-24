#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <Common/getRandomASCIIString.h>
#include <base/find_symbols.h>

#include <fmt/format.h>

#include <algorithm>
#include <vector>

namespace DB
{

PlainRewritableLayout::PlainRewritableLayout(std::string object_storage_common_key_prefix_)
    : object_storage_common_key_prefix(object_storage_common_key_prefix_)
{
}

std::string PlainRewritableLayout::constructMetadataDirectoryKey() const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN;
}

std::string PlainRewritableLayout::constructRootFilesDirectoryKey() const
{
    return object_storage_common_key_prefix / ROOT_DIRECTORY_TOKEN;
}

std::string PlainRewritableLayout::constructFilesDirectoryKey(const std::string & directory_remote_path) const
{
    return object_storage_common_key_prefix / directory_remote_path;
}

std::string PlainRewritableLayout::constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const
{
    return object_storage_common_key_prefix / directory_remote_path / file_name;
}

std::string PlainRewritableLayout::constructDirectoryObjectKey(const std::string & directory_remote_path) const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / directory_remote_path / PREFIX_PATH_FILE_NAME;
}

std::string PlainRewritableLayout::generateRemovedName()
{
    return REMOVED_NAME_PREFIX + getRandomASCIIString(REMOVED_NAME_RANDOM_PART_SIZE);
}

bool PlainRewritableLayout::isRemovedName(std::string_view name)
{
    if (name.size() != REMOVED_NAME_PREFIX.size() + REMOVED_NAME_RANDOM_PART_SIZE)
        return false;

    if (!name.starts_with(REMOVED_NAME_PREFIX))
        return false;

    /// The alphabet of `getRandomASCIIString`.
    return std::all_of(name.begin() + REMOVED_NAME_PREFIX.size(), name.end(), [](char c) { return 'a' <= c && c <= 'z'; });
}

bool PlainRewritableLayout::isRemovedLocalPath(const std::string & local_path)
{
    return getRemovedNameOfLocalPath(local_path).has_value();
}

std::optional<std::string> PlainRewritableLayout::getRemovedNameOfLocalPath(const std::string & local_path)
{
    const auto normalized_path = normalizePath(local_path);
    if (normalized_path.empty())
        return std::nullopt;

    auto name = normalized_path.begin()->string();
    if (!isRemovedName(name))
        return std::nullopt;

    return name;
}

std::string PlainRewritableLayout::restoreLocalPathOfPendingRemoval(const std::string & local_path, const std::string & original_local_path)
{
    const auto normalized_path = normalizePath(local_path);
    std::filesystem::path relative_path;
    for (auto it = std::next(normalized_path.begin()); it != normalized_path.end(); ++it)
        relative_path /= *it;

    /// The same shape as the paths `MoveDirectoryOperation` writes: `path_from / subdir / ""`.
    return (std::filesystem::path(original_local_path) / relative_path / "").string();
}

std::string PlainRewritableLayout::makePendingTombstoneContent(const std::string & original_local_path)
{
    return PENDING_TOMBSTONE_PREFIX + original_local_path;
}

std::optional<std::string> PlainRewritableLayout::parsePendingTombstoneContent(std::string_view content)
{
    if (!content.starts_with(PENDING_TOMBSTONE_PREFIX))
        return std::nullopt;

    return std::string(content.substr(PENDING_TOMBSTONE_PREFIX.size()));
}

std::string PlainRewritableLayout::constructTombstoneDirectoryKey() const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / TOMBSTONE_DIRECTORY_TOKEN;
}

std::string PlainRewritableLayout::constructTombstoneMarkerKey(const std::string & removed_name) const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / TOMBSTONE_DIRECTORY_TOKEN / removed_name;
}

std::optional<std::string> PlainRewritableLayout::parseTombstoneMarkerKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 3)
        return std::nullopt;

    const size_t size = key_parts.size();
    if (key_parts[size - 3] != METADATA_DIRECTORY_TOKEN || key_parts[size - 2] != TOMBSTONE_DIRECTORY_TOKEN)
        return std::nullopt;

    if (!isRemovedName(key_parts[size - 1]))
        return std::nullopt;

    return std::move(key_parts[size - 1]);
}

std::optional<std::pair<std::string, std::string>> PlainRewritableLayout::parseFileObjectKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 2)
        return std::nullopt;

    const size_t size = key_parts.size();
    return std::make_pair(std::move(key_parts[size - 2]), std::move(key_parts[size - 1]));
}

std::optional<std::string> PlainRewritableLayout::parseDirectoryObjectKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 3)
        return std::nullopt;

    const size_t size = key_parts.size();
    if (key_parts[size - 3] != METADATA_DIRECTORY_TOKEN)
        return std::nullopt;
    else if (key_parts[size - 1] != PREFIX_PATH_FILE_NAME)
        return std::nullopt;

    return std::move(key_parts[size - 2]);
}

}
