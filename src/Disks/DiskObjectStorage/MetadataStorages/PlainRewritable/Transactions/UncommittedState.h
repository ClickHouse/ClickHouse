#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>

namespace DB
{

class UncommittedState
{
    class PathResolver;

public:
    explicit UncommittedState(std::shared_ptr<FsSnapshot> tx_snapshot_);

    void useDirectory(const std::string & path) const;
    void useMissingDirectory(const std::string & path) const;

    void createDirectory(const std::string & path);
    void removeDirectory(const std::string & path);
    void moveDirectory(const std::string & path_from, const std::string & path_to);

    /// The file operations that add a file to a directory are replayed on the uncommitted state, because the choice
    /// of the object key for the next blob of a directory depends on them: a blob must never be written to the key
    /// of a blob that another file is going to keep alive. Removals are not replayed - a file that is still visible
    /// only makes the choice of the key more conservative.
    void recordCreatedFile(const std::string & path, const std::string & blob_key);
    /// A hard link: the target references the blob of the source, so the blob becomes shared and the target directory
    /// gets an explicit file list.
    void recordHardLink(const std::string & path_from, const std::string & path_to);
    /// A file move (or replace). `keeps_blob` tells that the blob stays where it is (a metadata-only move) instead of
    /// being copied to the default location in the target directory.
    void recordMovedFile(const std::string & path_from, const std::string & path_to, bool keeps_blob);
    void markDirectoryExplicit(const std::string & path);

    std::optional<DirectoryRemoteInfo> getDirectoryRemoteInfo(const std::string & path) const;
    const FsSnapshot & getSnapshot() const { return *tx_snapshot; }
    std::shared_ptr<Preconditions> getTxPreconditions() const;

private:
    std::shared_ptr<FsSnapshot> tx_snapshot;
    std::shared_ptr<Preconditions> preconditions;
    std::shared_ptr<PathResolver> path_resolver;
};

}
