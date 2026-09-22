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
    /// Blobs an operation writes aside to be able to roll itself back. `load` does not look into
    /// this prefix, so a scratch blob a rolled-back operation deliberately leaves behind (one that
    /// could only be deleted by a blind delete by path) does not come back as a file of the disk.
    constexpr static std::string SCRATCH_DIRECTORY_TOKEN = "__tmp";

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

    std::string constructMetadataDirectoryKey() const;
    std::string constructRootFilesDirectoryKey() const;
    std::string constructScratchFileObjectKey(const std::string & file_name) const;
    std::string constructFilesDirectoryKey(const std::string & directory_remote_path) const;
    std::string constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    std::string constructDirectoryObjectKey(const std::string & directory_remote_path) const;

    std::optional<std::pair<std::string, std::string>> parseFileObjectKey(const std::string & key) const;
    std::optional<std::string> parseDirectoryObjectKey(const std::string & key) const;

private:
    const std::filesystem::path object_storage_common_key_prefix;
};

}
