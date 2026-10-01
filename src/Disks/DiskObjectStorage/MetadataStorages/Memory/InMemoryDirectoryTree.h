#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

/// Directory tree of file metadata: pure path topology, no blob lifecycle. An empty path is the
/// root directory. Every mutating operation enforces that the parent exists and is a directory,
/// matching the on-disk metadata backends. Not thread-safe.
class InMemoryDirectoryTree
{
public:
    bool existsFile(const NormalizedPath & path) const;
    bool existsDirectory(const NormalizedPath & path) const;
    bool existsFileOrDirectory(const NormalizedPath & path) const;

    /// The metadata of the file at `path`, or nullptr.
    DiskObjectStorageMetadata * getMetadata(const NormalizedPath & path);
    const DiskObjectStorageMetadata * getMetadata(const NormalizedPath & path) const;

    /// Children of the directory at `path` as full paths, in lexicographic order; throws if
    /// there is no directory there.
    std::vector<std::string> listDirectory(const NormalizedPath & path) const;

    /// Insert a file at `path`, returning the metadata it displaced (if any); throws if the
    /// parent is missing or a directory occupies `path`.
    std::optional<DiskObjectStorageMetadata> putFile(const NormalizedPath & path, DiskObjectStorageMetadata metadata);

    /// Detach the file at `path` and return its metadata; throws if there is no file there.
    DiskObjectStorageMetadata removeFile(const NormalizedPath & path);

    /// Create the directory at `path` under an existing parent; an existing directory is kept,
    /// a file with this name throws.
    void createDirectory(const NormalizedPath & path);

    /// Create the directory at `path` and every missing parent; a file component throws.
    void createDirectoryRecursive(const NormalizedPath & path);

    /// Remove the directory at `path`; throws if it is missing, a file, or non-empty.
    void removeDirectory(const NormalizedPath & path);

    /// Detach the whole subtree at `path`, visiting every file metadata with its path relative to
    /// `path` ("." for a file at `path` itself). A missing `path` is a no-op.
    void removeSubtree(const NormalizedPath & path, const std::function<void(const std::string &, DiskObjectStorageMetadata &)> & visitor);

    /// Move the file at `from` to `to`. With `replace`, an existing destination file is
    /// displaced and its metadata returned; without it, any existing destination throws.
    std::optional<DiskObjectStorageMetadata> moveFile(const NormalizedPath & from, const NormalizedPath & to, bool replace);

    /// Relink the directory at `from` to `to`, which must not exist and must not lie inside
    /// `from`.
    void moveDirectory(const NormalizedPath & from, const NormalizedPath & to);

    /// Visit every file metadata under the directory at `path` with its full path.
    void forEachMetadataUnder(const NormalizedPath & path, const std::function<void(const std::string &, DiskObjectStorageMetadata &)> & visitor) const;

private:
    /// A file (holds metadata) or a directory (holds children).
    struct Node
    {
        std::optional<DiskObjectStorageMetadata> metadata;
        std::map<std::string, std::shared_ptr<Node>> children;

        bool isFile() const { return metadata.has_value(); }
    };
    using NodePtr = std::shared_ptr<Node>;

    /// The node at `path`, or nullptr if any component is missing or a file stands in the way.
    NodePtr resolve(const NormalizedPath & path) const;

    /// The directory that holds `path`'s last component, plus the leaf name; throws unless
    /// every parent component exists and is a directory, or if `path` is the root.
    std::pair<Node *, std::string> resolveParent(const NormalizedPath & path) const;

    NodePtr root = std::make_shared<Node>();
};

}
