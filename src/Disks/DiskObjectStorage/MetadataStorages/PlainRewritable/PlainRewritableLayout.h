#pragma once

#include <string>
#include <optional>
#include <filesystem>

namespace DB
{

class PlainRewritableLayout
{
public:
    constexpr static std::string PREFIX_PATH_FILE_NAME = "prefix.path";
    constexpr static std::string METADATA_DIRECTORY_TOKEN = "__meta";
    constexpr static std::string ROOT_DIRECTORY_TOKEN = "__root";

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

    std::string constructMetadataDirectoryKey() const;
    std::string constructRootFilesDirectoryKey() const;
    std::string constructFilesDirectoryKey(const std::string & directory_remote_path) const;
    std::string constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    /// Absolute object key from a relative key stored in explicit prefix.path.
    std::string constructObjectKey(const std::string & relative_object_key) const;
    /// Relative object key (without common key prefix) for a file in an implicit directory layout.
    std::string makeRelativeFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    /// Strip the common key prefix from an absolute object key.
    std::string makeRelativeObjectKey(const std::string & absolute_object_key) const;
    std::string constructDirectoryObjectKey(const std::string & directory_remote_path) const;

    std::optional<std::pair<std::string, std::string>> parseFileObjectKey(const std::string & key) const;
    std::optional<std::string> parseDirectoryObjectKey(const std::string & key) const;

private:
    const std::filesystem::path object_storage_common_key_prefix;
};

}
