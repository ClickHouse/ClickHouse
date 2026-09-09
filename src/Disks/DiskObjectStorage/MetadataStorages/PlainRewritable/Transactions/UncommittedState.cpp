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

void UncommittedState::recordCreatedFile(const std::string & path, const std::string & blob_key)
{
    const auto normalized_path = normalizePath(path);
    const auto directory = tx_snapshot->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory)
        return;

    const auto file_name = normalized_path.filename().string();
    FileRemoteInfo info{.blob_key = blob_key};
    const auto new_blob_key = getBlobKey(*directory, file_name, info);

    /// Rewriting a file: it stops referencing its previous blob, unless the blob is reused in place.
    if (const auto it = directory->files.find(file_name); it != directory->files.end())
    {
        const auto previous_blob_key = getBlobKey(*directory, file_name, it->second);
        /// The count is tracked only for the blobs that have more than one link, so the last link is not subtracted.
        if (previous_blob_key != new_blob_key && tx_snapshot->getBlobLinkCount(previous_blob_key) > 1)
            tx_snapshot->removeBlobLink(previous_blob_key);

        tx_snapshot->removeFile(path);
    }

    tx_snapshot->recordFile(path, std::move(info));

    /// A blob outside of the default location cannot be found by listing the prefix of the directory, so the commit
    /// switches the directory to the explicit file list. Without recording that here, the next blob of this directory
    /// would be placed at the default location again - which is exactly the blob that the file this one was
    /// hard-linked from is keeping alive.
    if (new_blob_key != getDefaultBlobKey(directory->remote_path, file_name))
        tx_snapshot->markDirectoryExplicit(normalized_path.parent_path());
}

void UncommittedState::recordHardLink(const std::string & path_from, const std::string & path_to)
{
    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);

    /// The target directory gains a file whose blob is stored under the prefix of another directory.
    markDirectoryExplicit(normalized_path_to.parent_path());

    const auto directory_from = tx_snapshot->getDirectoryRemoteInfo(normalized_path_from.parent_path());
    if (!directory_from)
        return;

    const auto file_name_from = normalized_path_from.filename().string();
    const auto file_from = directory_from->files.find(file_name_from);
    if (file_from == directory_from->files.end())
        return;

    auto info = file_from->second;
    info.blob_key = getBlobKey(*directory_from, file_name_from, file_from->second);
    tx_snapshot->addBlobLink(info.blob_key);

    /// A hard link over something that already exists is rejected by the commit, which is where the error belongs.
    if (!tx_snapshot->getDirectoryRemoteInfo(normalized_path_to.parent_path())
        || tx_snapshot->existsFile(path_to)
        || tx_snapshot->existsDirectory(path_to))
        return;

    tx_snapshot->recordFile(path_to, std::move(info));
}

void UncommittedState::recordMovedFile(const std::string & path_from, const std::string & path_to, bool keeps_blob)
{
    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    if (normalized_path_from == normalized_path_to)
        return;

    const auto directory_from = tx_snapshot->getDirectoryRemoteInfo(normalized_path_from.parent_path());
    const auto directory_to = tx_snapshot->getDirectoryRemoteInfo(normalized_path_to.parent_path());
    if (!directory_from || !directory_to)
        return;

    const auto file_name_from = normalized_path_from.filename().string();
    const auto file_name_to = normalized_path_to.filename().string();

    const auto file_from = directory_from->files.find(file_name_from);
    if (file_from == directory_from->files.end())
        return;

    /// A move over a directory is rejected by the commit, which is where the error belongs.
    if (tx_snapshot->existsDirectory(path_to))
        return;

    auto info = file_from->second;
    /// A metadata-only move leaves the blob where it is; otherwise the blob is copied to the default location
    /// in the target directory.
    info.blob_key = keeps_blob
        ? getBlobKey(*directory_from, file_name_from, file_from->second)
        : getDefaultBlobKey(directory_to->remote_path, file_name_to);

    /// The replaced file stops referencing its blob. The blob itself survives only when it is shared:
    /// a move that is not metadata-only overwrites it.
    if (const auto file_to = directory_to->files.find(file_name_to); file_to != directory_to->files.end())
    {
        const auto replaced_blob_key = getBlobKey(*directory_to, file_name_to, file_to->second);
        /// The count is tracked only for the blobs that have more than one link, so the last link is not subtracted.
        if (tx_snapshot->getBlobLinkCount(replaced_blob_key) > 1)
            tx_snapshot->removeBlobLink(replaced_blob_key);

        tx_snapshot->removeFile(path_to);
    }

    tx_snapshot->removeFile(path_from);
    tx_snapshot->recordFile(path_to, std::move(info));
}

void UncommittedState::markDirectoryExplicit(const std::string & path)
{
    if (!tx_snapshot->getDirectoryRemoteInfo(path))
        return;

    tx_snapshot->markDirectoryExplicit(path);
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
