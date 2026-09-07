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

    /// File operations are not replayed on the uncommitted state, except for the facts that affect
    /// the choice of object keys for new blobs: the files that are going to be created together with the keys
    /// of their blobs, the directories that are going to get an explicit file list, and the blobs
    /// that are going to be shared.
    void recordCreatedFile(const std::string & path, const std::string & blob_key);
    void markDirectoryExplicit(const std::string & path);
    void addBlobLink(const std::string & blob_key);

    std::optional<DirectoryRemoteInfo> getDirectoryRemoteInfo(const std::string & path) const;
    const FsSnapshot & getSnapshot() const { return *tx_snapshot; }
    std::shared_ptr<Preconditions> getTxPreconditions() const;

private:
    std::shared_ptr<FsSnapshot> tx_snapshot;
    std::shared_ptr<Preconditions> preconditions;
    std::shared_ptr<PathResolver> path_resolver;
};

}
