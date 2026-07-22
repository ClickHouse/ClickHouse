#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>

namespace DB
{

class UncommittedState
{
    class PathResolver;

    void useDirectory(const std::string & path) const;
    void useMissingDirectory(const std::string & path) const;

public:
    explicit UncommittedState(std::shared_ptr<FsSnapshot> tx_snapshot_);

    void createDirectory(const std::string & path);
    void removeDirectory(const std::string & path);
    void moveDirectory(const std::string & path_from, const std::string & path_to);

    std::pair<bool, std::optional<DirectoryRemoteInfo>> lookupDirectory(const std::string & path) const;
    std::shared_ptr<Preconditions> getTxPreconditions() const;

private:
    std::shared_ptr<FsSnapshot> tx_snapshot;
    std::shared_ptr<Preconditions> preconditions;
    std::shared_ptr<PathResolver> path_resolver;
};

}
