#pragma once

#include <string>
#include <optional>

namespace DB
{

class PlainRewritableLayout
{
public:
    constexpr static std::string PREFIX_PATH_FILE_NAME = "prefix.path";
    constexpr static std::string METADATA_DIRECTORY_TOKEN = "__meta";
    constexpr static std::string ROOT_DIRECTORY_TOKEN = "__root";

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

    /// TODO:
    std::string getMetadataDirectoryKey() const;
    std::string getRootFilesDirectoryKey() const;
    std::string getFilesDirectoryKey(const std::string & directory_remote_path) const;

    /// TODO:
    std::string packFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    std::string packDirectoryObjectKey(const std::string & directory_remote_path) const;

    /// TODO:
    std::optional<std::pair<std::string, std::string>> unpackFileObjectKey(const std::string & key) const;
    std::optional<std::string> unpackDirectoryObjectKey(const std::string & key) const;

private:
    const std::string object_storage_common_key_prefix;
};

}
