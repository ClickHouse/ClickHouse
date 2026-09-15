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
    /// Holds the markers of the removals that have been committed but not finished, see `REMOVED_NAME_PREFIX`.
    /// It lives under `__meta`, where a name of this shape can never be the remote path of a directory,
    /// and the objects in it are ignored by `parseDirectoryObjectKey`.
    constexpr static std::string TOMBSTONE_DIRECTORY_TOKEN = "__tombstone";

    /// A local name of the shape of `generateRemovedName` (of a top-level directory, or of a file in the root
    /// directory) belongs to a removal that has been committed but not finished yet:
    /// - `RemoveRecursive` moves the subtree being removed under such a name (by rewriting `prefix.path`
    ///   of every directory in it), commits, and only then deletes the objects;
    /// - file operations keep a backup copy of a file they remove or overwrite under such a name in `__root`,
    ///   to be able to undo, and delete it after the commit.
    /// If the process dies in between, these objects are garbage: they are skipped when the metadata is loaded,
    /// and deleted during the initial load. Such names are reserved: an attempt to create them is rejected.
    ///
    /// The shape of the name alone never means anything: a name of this shape could have been created as
    /// ordinary data by an older version, which reserved nothing, so treating it as garbage would delete or
    /// hide the data of such a disk after an upgrade. What makes a name garbage is an explicit marker object
    /// (`constructTombstoneMarkerKey`), written before the removal is committed and deleted after its objects
    /// are gone. Only a name that has one is skipped on load and reclaimed; everything else is ordinary data.
    constexpr static std::string REMOVED_NAME_PREFIX = "__removed.";
    constexpr static size_t REMOVED_NAME_RANDOM_PART_SIZE = 16;

    static std::string generateRemovedName();
    static bool isRemovedName(std::string_view name);
    /// Whether the first component of a local path (as stored in `prefix.path`) is a removed name.
    static bool isRemovedLocalPath(const std::string & local_path);
    /// The first component of a local path (as stored in `prefix.path`), if it is a removed name.
    static std::optional<std::string> getRemovedNameOfLocalPath(const std::string & local_path);

    explicit PlainRewritableLayout(std::string object_storage_common_key_prefix_);

    std::string constructMetadataDirectoryKey() const;
    std::string constructRootFilesDirectoryKey() const;
    std::string constructFilesDirectoryKey(const std::string & directory_remote_path) const;
    std::string constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const;
    std::string constructDirectoryObjectKey(const std::string & directory_remote_path) const;
    std::string constructTombstoneDirectoryKey() const;
    std::string constructTombstoneMarkerKey(const std::string & removed_name) const;

    std::optional<std::pair<std::string, std::string>> parseFileObjectKey(const std::string & key) const;
    std::optional<std::string> parseDirectoryObjectKey(const std::string & key) const;
    std::optional<std::string> parseTombstoneMarkerKey(const std::string & key) const;

private:
    const std::filesystem::path object_storage_common_key_prefix;
};

}
