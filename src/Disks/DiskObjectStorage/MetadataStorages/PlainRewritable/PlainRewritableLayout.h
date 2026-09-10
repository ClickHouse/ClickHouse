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

    /// Prefix of the names used as copy/rename targets while a mutating operation is in flight (file
    /// move/unlink, recursive directory removal). It makes such an object identifiable from its name alone,
    /// so a leftover of an interrupted operation can be told from real data by looking at the path - see
    /// `system.remote_data_paths`. Names carrying no data have no reason to be indistinguishable from ones
    /// that do.
    constexpr static std::string TEMP_NAME_PREFIX = "_tmp_";

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

    /// A fresh name for an object that only exists for the duration of one operation.
    static std::string generateTempName();

    std::string constructMetadataDirectoryKey() const;
    std::string constructRootFilesDirectoryKey() const;
    std::string constructFilesDirectoryKey(const std::string & directory_remote_path) const;
    std::string constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    /// Object key of a blob given its key relative to the common key prefix (see `FileRemoteInfo::blob_key`).
    std::string constructBlobObjectKey(const std::string & blob_key) const;
    std::string constructDirectoryObjectKey(const std::string & directory_remote_path) const;

    std::optional<std::pair<std::string, std::string>> parseFileObjectKey(const std::string & key) const;
    std::optional<std::string> parseDirectoryObjectKey(const std::string & key) const;

private:
    const std::filesystem::path object_storage_common_key_prefix;
};

}
