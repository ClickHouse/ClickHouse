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

    /// A local name of exactly the shape of `generateRemovedName` (of a top-level directory, or of a file in the
    /// root directory) belongs to a removal that has been committed but not finished yet:
    /// - `RemoveRecursive` moves the subtree being removed under such a name (by rewriting `prefix.path`
    ///   of every directory in it), commits, and only then deletes the objects;
    /// - file operations keep a backup copy of a file they remove or overwrite under such a name in `__root`,
    ///   to be able to undo, and delete it after the commit.
    /// If the process dies in between, these objects are garbage: they are skipped when the metadata is loaded,
    /// and deleted during the initial load. Such names are reserved: an attempt to create them is rejected.
    ///
    /// Only the exact shape is recognized, and not everything starting with the prefix, because a name that
    /// merely starts with it could have been created as ordinary data before the shape became reserved:
    /// no version has ever produced a logical name of this shape (the random names of the same alphabet that
    /// earlier versions used for the same purpose carry no prefix), and creating one is rejected from now on.
    constexpr static std::string REMOVED_NAME_PREFIX = "__removed.";
    constexpr static size_t REMOVED_NAME_RANDOM_PART_SIZE = 16;

    static std::string generateRemovedName();
    static bool isRemovedName(std::string_view name);
    /// Whether the first component of a local path (as stored in `prefix.path`) is a removed name.
    static bool isRemovedLocalPath(const std::string & local_path);

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

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
