#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>

#include <base/find_symbols.h>

#include <fmt/format.h>

#include <vector>

namespace DB
{

PlainRewritableLayout::PlainRewritableLayout(std::string object_storage_common_key_prefix_)
    : object_storage_common_key_prefix(std::move(object_storage_common_key_prefix_))
{
}

std::string PlainRewritableLayout::getMetadataDirectoryKey() const
{
    return fmt::format("{}/{}", object_storage_common_key_prefix, METADATA_DIRECTORY_TOKEN);
}

std::string PlainRewritableLayout::getRootFilesDirectoryKey() const
{
    return fmt::format("{}/{}", object_storage_common_key_prefix, ROOT_DIRECTORY_TOKEN);
}

std::string PlainRewritableLayout::getFilesDirectoryKey(const std::string & directory_remote_path) const
{
    return fmt::format("{}/{}", object_storage_common_key_prefix, directory_remote_path);
}

std::string PlainRewritableLayout::packFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const
{
    return fmt::format("{}/{}/{}", object_storage_common_key_prefix, directory_remote_path, file_name);
}

std::string PlainRewritableLayout::packDirectoryObjectKey(const std::string & directory_remote_path) const
{
    return fmt::format("{}/{}/{}/{}", object_storage_common_key_prefix, METADATA_DIRECTORY_TOKEN, directory_remote_path, PREFIX_PATH_FILE_NAME);
}

std::optional<std::pair<std::string, std::string>> PlainRewritableLayout::unpackFileObjectKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 3)
        return std::nullopt;

    const size_t size = key_parts.size();
    return std::make_pair(std::move(key_parts[size - 2]), std::move(key_parts[size - 1]));
}

std::optional<std::string> PlainRewritableLayout::unpackDirectoryObjectKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 4)
        return std::nullopt;

    const size_t size = key_parts.size();
    if (key_parts[size - 3] != METADATA_DIRECTORY_TOKEN)
        return std::nullopt;
    else if (key_parts[size - 1] != PREFIX_PATH_FILE_NAME)
        return std::nullopt;

    return std::move(key_parts[size - 2]);
}

}
