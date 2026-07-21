#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>

#include <base/find_symbols.h>

#include <fmt/format.h>

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

std::string PlainRewritableLayout::constructObjectKey(const std::string & relative_object_key) const
{
    return object_storage_common_key_prefix / relative_object_key;
}

std::string PlainRewritableLayout::makeRelativeFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const
{
    return (std::filesystem::path(directory_remote_path) / file_name).string();
}

std::string PlainRewritableLayout::makeRelativeObjectKey(const std::string & absolute_object_key) const
{
    const auto absolute = std::filesystem::path(absolute_object_key).lexically_normal().string();
    const auto prefix = object_storage_common_key_prefix.lexically_normal().string();

    std::string_view relative = absolute;
    if (relative.starts_with(prefix))
    {
        relative.remove_prefix(prefix.size());
        while (relative.starts_with('/'))
            relative.remove_prefix(1);
        return std::string(relative);
    }

    /// Already relative (or unexpected layout) — return as-is without a leading "./".
    while (relative.starts_with("./"))
        relative.remove_prefix(2);
    while (relative.starts_with('/'))
        relative.remove_prefix(1);
    return std::string(relative);
}

std::string PlainRewritableLayout::constructDirectoryObjectKey(const std::string & directory_remote_path) const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / directory_remote_path / PREFIX_PATH_FILE_NAME;
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
