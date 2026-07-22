#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <base/defines.h>

#include <Common/getRandomASCIIString.h>

#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace DB
{

class UncommittedState::PathResolver
{
    struct Move
    {
        NormalizedPath from = {};
        NormalizedPath to = {};
    };

    struct Remove
    {
        NormalizedPath path = {};
    };

    using Event = std::variant<Move, Remove>;

public:
    void recordMove(const NormalizedPath & from, const NormalizedPath & to)
    {
        events.push_back(Move{.from = from, .to = to});
    }

    void recordRemove(const NormalizedPath & directory)
    {
        events.push_back(Remove{.path = directory});
    }

    void recordCreate(const NormalizedPath & directory)
    {
        if (const auto snapshot_path = resolveToSnapshotPath(directory))
            created_directories.insert(*snapshot_path);
    }

    std::optional<NormalizedPath> resolveToSnapshotPath(const NormalizedPath & path) const
    {
        auto resolved = path.string();

        for (const auto & event : events | std::views::reverse)
        {
            if (const Remove * remove = std::get_if<Remove>(&event))
            {
                if (resolved == remove->path || resolved.starts_with(remove->path.native() + '/'))
                    return std::nullopt;
            }
            else if (const Move * move = std::get_if<Move>(&event))
            {
                if (resolved == move->to)
                    resolved = move->from;
                else if (resolved.starts_with(move->to.native() + '/'))
                    resolved = move->from.native() + resolved.substr(move->to.native().size());
                else if (resolved == move->from.native() || resolved.starts_with(move->from.native() + '/'))
                    return std::nullopt;
            }
            else
            {
                UNREACHABLE();
            }
        }

        return NormalizedPath{resolved};
    }

    bool isCreatedByTransaction(const NormalizedPath & path) const
    {
        return created_directories.contains(path);
    }

private:
    std::vector<Event> events;
    std::unordered_set<std::string> created_directories;
};

void UncommittedState::useDirectory(const std::string & path) const
{
    const auto info = tx_snapshot->getDirectoryRemoteInfo(path);
    if (!info)
        return;

    const auto snapshot_path = path_resolver->resolveToSnapshotPath(normalizePath(path));
    if (snapshot_path && !path_resolver->isCreatedByTransaction(*snapshot_path))
        preconditions->checkDirectoryPresent(*snapshot_path, info->remote_path);
}

void UncommittedState::useMissingDirectory(const std::string & path) const
{
    if (const auto snapshot_path = path_resolver->resolveToSnapshotPath(normalizePath(path)))
        preconditions->checkDirectoryMissing(*snapshot_path);
}

UncommittedState::UncommittedState(std::shared_ptr<FsSnapshot> tx_snapshot_)
    : tx_snapshot(std::move(tx_snapshot_))
    , preconditions(std::make_shared<Preconditions>())
    , path_resolver(std::make_shared<PathResolver>())
{
}

void UncommittedState::createDirectory(const std::string & path)
{
    if (tx_snapshot->getDirectoryRemoteInfo(path))
    {
        useDirectory(path);
        return;
    }

    useMissingDirectory(path);
    path_resolver->recordCreate(normalizePath(path));
    tx_snapshot->recordDirectoryPath(path, DirectoryRemoteInfo{ .remote_path = getRandomASCIIString(32), .etag = "", .files = {}});
}

void UncommittedState::removeDirectory(const std::string & path)
{
    if (!tx_snapshot->existsDirectory(path).first)
    {
        useMissingDirectory(path);
        return;
    }

    useDirectory(path);
    path_resolver->recordRemove(normalizePath(path));
    tx_snapshot->removeDirectory(path);
}

void UncommittedState::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    if (!tx_snapshot->existsDirectory(path_from).first)
    {
        useMissingDirectory(path_from);
        return;
    }

    if (tx_snapshot->existsDirectory(path_to).first || tx_snapshot->existsFile(path_to))
    {
        useDirectory(path_to);
        return;
    }

    useDirectory(path_from);
    path_resolver->recordMove(normalizePath(path_from), normalizePath(path_to));
    tx_snapshot->moveDirectory(path_from, path_to);
}

std::pair<bool, std::optional<DirectoryRemoteInfo>> UncommittedState::lookupDirectory(const std::string & path) const
{
    useDirectory(path);
    return tx_snapshot->existsDirectory(path);
}

std::shared_ptr<Preconditions> UncommittedState::getTxPreconditions() const
{
    return preconditions;
}

}
