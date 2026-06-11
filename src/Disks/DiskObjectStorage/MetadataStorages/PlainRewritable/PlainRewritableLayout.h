#pragma once

#include <string>
#include <string_view>
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

    /// Length of randomly generated directory names backing logical directories.
    constexpr static size_t DIRECTORY_REMOTE_NAME_LENGTH = 32;
    /// Length of randomly generated ephemeral (temporary) names used as copy/rename targets
    /// during mutating operations (file move/unlink and recursive directory removal).
    constexpr static size_t EPHEMERAL_TEMP_NAME_LENGTH = 16;

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

    /// Whether `name` looks like an ephemeral temporary name produced during a mutating operation.
    /// Used to flag leftover blobs of interrupted operations (see `system.plain_rewritable_data_paths`).
    static bool looksLikeEphemeralName(std::string_view name);

    std::string constructMetadataDirectoryKey() const;
    std::string constructRootFilesDirectoryKey() const;
    std::string constructFilesDirectoryKey(const std::string & directory_remote_path) const;
    std::string constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    std::string constructDirectoryObjectKey(const std::string & directory_remote_path) const;

    std::optional<std::pair<std::string, std::string>> parseFileObjectKey(const std::string & key) const;
    std::optional<std::string> parseDirectoryObjectKey(const std::string & key) const;

private:
    const std::filesystem::path object_storage_common_key_prefix;
};

}
