#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

/// Directory tree of file records: pure path topology, no blob lifecycle. Paths are
/// '/'-separated, "" is the root directory. Every mutating operation enforces that the parent
/// exists and is a directory, matching the on-disk metadata backends. Not thread-safe.
class InMemoryDirectoryTree
{
public:
    using Record = DiskObjectStorageMetadata;

    bool existsFile(std::string_view path) const;
    bool existsDirectory(std::string_view path) const;
    bool existsFileOrDirectory(std::string_view path) const;

    /// The record of the file at `path`, or nullptr.
    Record * getRecord(std::string_view path);
    const Record * getRecord(std::string_view path) const;

    /// Children of the directory at `path` as full paths, in lexicographic order; throws if
    /// there is no directory there.
    std::vector<std::string> listDirectory(std::string_view path) const;

    /// Insert a file at `path`, returning the record it displaced (if any); throws if the
    /// parent is missing or a directory occupies `path`.
    std::optional<Record> putFile(std::string_view path, Record record);

    /// Detach the file at `path` and return its record; throws if there is no file there.
    Record removeFile(std::string_view path);

    /// Create the directory at `path` under an existing parent; an existing directory is kept,
    /// a file with this name throws.
    void createDirectory(std::string_view path);

    /// Create the directory at `path` and every missing parent; a file component throws.
    void createDirectoryRecursive(std::string_view path);

    /// Remove the directory at `path`; throws if it is missing, a file, or non-empty.
    void removeDirectory(std::string_view path);

    /// Detach the whole subtree at `path`, visiting every file record with its path relative to
    /// `path` ("." for a file at `path` itself). A missing `path` is a no-op.
    void removeSubtree(std::string_view path, const std::function<void(const std::string &, Record &)> & visitor);

    /// Move the file at `from` to `to`. With `replace`, an existing destination file is
    /// displaced and returned; without it, any existing destination throws.
    std::optional<Record> moveFile(std::string_view from, std::string_view to, bool replace);

    /// Relink the directory at `from` to `to`, which must not exist and must not lie inside
    /// `from`.
    void moveDirectory(std::string_view from, std::string_view to);

    /// Visit every file record under the directory at `path` with its full path.
    void forEachRecordUnder(std::string_view path, const std::function<void(const std::string &, Record &)> & visitor) const;

private:
    /// A file (holds a record) or a directory (holds children).
    struct Node
    {
        std::optional<Record> record;
        std::map<std::string, std::shared_ptr<Node>> children;

        bool isFile() const { return record.has_value(); }
    };
    using NodePtr = std::shared_ptr<Node>;

    /// The node at `path`, or nullptr if any component is missing or a file stands in the way.
    NodePtr resolve(std::string_view path) const;

    /// The directory that holds `path`'s last component, plus the leaf name; throws unless
    /// every parent component exists and is a directory, or if `path` is the root.
    std::pair<Node *, std::string> resolveParent(std::string_view path) const;

    NodePtr root = std::make_shared<Node>();
};

}
