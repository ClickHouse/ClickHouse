#include <base/pathToString.h>
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
            created_directories.insert(pathToGenericString(*snapshot_path));
    }

    std::optional<NormalizedPath> resolveToSnapshotPath(const NormalizedPath & path) const
    {
        /// The whole resolution happens in generic UTF-8 strings, including the starting point:
        /// `path.string()` is native-format on Windows, so `B\\C` would stop matching the `B/`
        /// prefix of a recorded move and the descendants of a moved directory would resolve to
        /// themselves rather than to their pre-move path.
        auto resolved = pathToGenericString(path);

        for (const auto & event : events | std::views::reverse)
        {
            if (const Remove * remove = std::get_if<Remove>(&event))
            {
                const auto removed = pathToGenericString(remove->path);
                if (resolved == removed || resolved.starts_with(removed + '/'))
                    return std::nullopt;
            }
            else if (const Move * move = std::get_if<Move>(&event))
            {
                const auto from = pathToGenericString(move->from);
                const auto to = pathToGenericString(move->to);
                if (resolved == to)
                    resolved = from;
                else if (resolved.starts_with(to + '/'))
                    resolved = from + resolved.substr(to.size());
                else if (resolved == from || resolved.starts_with(from + '/'))
                    return std::nullopt;
            }
            else
            {
                UNREACHABLE();
            }
        }

        return NormalizedPath{pathFromString(resolved)};
    }

    bool isCreatedByTransaction(const NormalizedPath & path) const
    {
        return created_directories.contains(pathToGenericString(path));
    }

private:
    std::vector<Event> events;
    std::unordered_set<std::string> created_directories;
};

UncommittedState::UncommittedState(std::shared_ptr<FsSnapshot> tx_snapshot_)
    : tx_snapshot(std::move(tx_snapshot_))
    , preconditions(std::make_shared<Preconditions>())
    , path_resolver(std::make_shared<PathResolver>())
{
}

void UncommittedState::useDirectory(const std::string & path) const
{
    const auto info = tx_snapshot->getDirectoryRemoteInfo(path);
    if (!info)
        return;

    const auto snapshot_path = path_resolver->resolveToSnapshotPath(normalizePath(path));
    if (!snapshot_path)
        return;

    if (path_resolver->isCreatedByTransaction(*snapshot_path))
        preconditions->checkDirectoryMissing(*snapshot_path);
    else
        preconditions->checkDirectoryPresent(*snapshot_path, info->remote_path);
}

void UncommittedState::useMissingDirectory(const std::string & path) const
{
    const auto info = tx_snapshot->getDirectoryRemoteInfo(path);
    if (info)
        return;

    const auto snapshot_path = path_resolver->resolveToSnapshotPath(normalizePath(path));
    if (snapshot_path)
        preconditions->checkDirectoryMissing(*snapshot_path);
}

void UncommittedState::createDirectory(const std::string & path)
{
    if (tx_snapshot->getDirectoryRemoteInfo(path))
        return;

    path_resolver->recordCreate(normalizePath(path));
    tx_snapshot->recordDirectoryPath(path, DirectoryRemoteInfo{ .remote_path = getRandomASCIIString(32), .etag = "", .files = {}});
}

void UncommittedState::removeDirectory(const std::string & path)
{
    if (!tx_snapshot->existsDirectory(path))
        return;

    path_resolver->recordRemove(normalizePath(path));
    tx_snapshot->removeDirectory(path);
}

void UncommittedState::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    if (!tx_snapshot->existsDirectory(path_from))
        return;

    if (tx_snapshot->existsDirectory(path_to) || tx_snapshot->existsFile(path_to))
        return;

    path_resolver->recordMove(normalizePath(path_from), normalizePath(path_to));
    tx_snapshot->moveDirectory(path_from, path_to);
}

std::optional<DirectoryRemoteInfo> UncommittedState::getDirectoryRemoteInfo(const std::string & path) const
{
    return tx_snapshot->getDirectoryRemoteInfo(path);
}

std::shared_ptr<Preconditions> UncommittedState::getTxPreconditions() const
{
    return preconditions;
}

}
