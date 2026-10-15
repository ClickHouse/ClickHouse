#include <Storages/FileLog/DirectoryWatcherBase.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Storages/FileLog/DirectoryWatchReactor.h>
#include <Storages/FileLog/FileLogSettings.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <base/defines.h>

#include <Interpreters/Context.h>

#include <filesystem>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#if defined(OS_LINUX)
#include <algorithm>
#include <deque>
#include <sys/inotify.h>
#include <chrono>
#include <unordered_set>
#elif defined(OS_DARWIN)
#include <map>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/stat.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int IO_SETUP_ERROR;
    extern const int LOGICAL_ERROR;
}

#if defined(OS_DARWIN)
namespace FileLogSetting
{
    extern const FileLogSettingsUInt64 poll_directory_watch_events_backoff_factor;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_init;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_max;
}
#endif

DirectoryWatcherBase::DirectoryWatcherBase(
    FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_)
    : WithContext(context_)
    , owner(owner_)
    , path(path_)
    , event_mask(event_mask_)
{
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Path {} does not exist", path);

    if (!std::filesystem::is_directory(path))
        throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path {} is not a directory", path);

    reactor = getContext()->getGlobalContext()->getDirectoryWatchReactor();
    try
    {
#if defined(OS_DARWIN)
        /// macOS has no inotify. We reconstruct the same event stream by diffing a filename ->
        /// inode snapshot of the directory. Tracking the inode lets us tell a rename
        /// (same inode reappearing under a new name) apart from a delete followed by a create, so the
        /// read offset of a renamed file is preserved just like with inotify's IN_MOVED_FROM/IN_MOVED_TO.
        ///
        /// The vnode event of the directory says that an entry was added, removed or renamed, but not which one, and
        /// it does not fire on appends to the files inside. Those are reported by the descriptors of the files, which
        /// `syncFileWatches` registers, and are reported as `DW_ITEM_MODIFIED` at once. The other events are used only to
        /// scan the directory at once (matching inotify's latency, which the no-sleep test sequences rely on), and the scan finds
        /// out what has changed. The watcher also scans on a timer with a backoff, so that a failed scan is retried and a
        /// change that no event reported is found anyway.
        ///
        /// Because the per-file `NOTE_DELETE` is load-bearing (see `DarwinDirectoryWatchState::deleted_files`), kqueue is
        /// required: we fail closed on any setup error (mirroring the Linux watcher, which throws when `inotify_init1`
        /// or `inotify_add_watch` fail) rather than degrade to a polling mode that would silently keep stale read offsets.
        const auto & settings = owner.storage.getFileLogSettings();
        watch_state = std::make_unique<DarwinDirectoryWatchState>(
            (*settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds());
        watch_state->followed_files = owner.read_files;

        watch_state->dir_fd = ::open(path.c_str(), O_EVTONLY);
        if (watch_state->dir_fd == -1)
            ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "Cannot open directory to watch");
#endif
        reactor->add(*this);
        registered = true;
    }
    catch (...)
    {
        /// The table stays alive without the watcher and reports the error to the reader, as it did when the watch
        /// thread failed to add the watch.
        owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "Watch directory {} failed: {}", path, getCurrentExceptionMessage(false)));
    }
}

#if defined(OS_LINUX)

/// An item moved out of the directory never gets its IN_MOVED_TO, so old cookies are evicted.
static constexpr size_t max_unpaired_move_cookies = 1024;

uint32_t DirectoryWatcherBase::watchMask() const
{
    uint32_t mask = 0;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_ADDED)
        mask |= IN_CREATE;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_REMOVED)
        mask |= IN_DELETE;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_MODIFIED)
        mask |= IN_MODIFY;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_FROM)
        mask |= IN_MOVED_FROM;
    if (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_TO)
        mask |= IN_MOVED_TO;
    return mask;
}

void DirectoryWatcherBase::onEvent(const inotify_event & event)
{
    const inotify_event * p_event = &event;
    if (p_event->len == 0)
        return;

    if ((p_event->mask & IN_CREATE) && (eventMask() & DirectoryWatcherBase::DW_ITEM_ADDED))
    {
        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_ADDED);
        owner.onItemAdded(ev);
    }
    if ((p_event->mask & IN_DELETE) && (eventMask() & DirectoryWatcherBase::DW_ITEM_REMOVED))
    {
        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_REMOVED);
        owner.onItemRemoved(ev);
    }
    if ((p_event->mask & IN_MODIFY) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MODIFIED))
    {
        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MODIFIED);
        owner.onItemModified(ev);
    }
    if ((p_event->mask & IN_MOVED_FROM) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_FROM))
    {
        unpaired_move_cookies.push_back(p_event->cookie);
        if (unpaired_move_cookies.size() > max_unpaired_move_cookies)
            unpaired_move_cookies.pop_front();
        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_FROM, p_event->cookie);
        owner.onItemMovedFrom(ev);
    }
    if (p_event->mask & IN_MOVED_TO)
    {
        /// Only a rename inside the directory delivers both halves. An item moved in from elsewhere
        /// is new here, even if it has the inode of a file just deleted from the directory.
        auto paired = std::find(unpaired_move_cookies.begin(), unpaired_move_cookies.end(), p_event->cookie);
        if (paired != unpaired_move_cookies.end())
        {
            unpaired_move_cookies.erase(paired);
            if (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_TO)
            {
                DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_TO, p_event->cookie);
                owner.onItemMovedTo(ev);
            }
        }
        else if (eventMask() & DirectoryWatcherBase::DW_ITEM_ADDED)
        {
            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_ADDED);
            owner.onItemAdded(ev);
        }
    }

    if (p_event->mask & IN_MOVED_FROM)
        unpaired_moves.insert(p_event->cookie);
    if (p_event->mask & IN_MOVED_TO)
        unpaired_moves.erase(p_event->cookie);
}

bool DirectoryWatcherBase::onReadEnd()
{
    /// The IN_MOVED_FROM and IN_MOVED_TO of one rename can come in two reads (see inotify(7)): hold the events back
    /// until every IN_MOVED_FROM has its IN_MOVED_TO, or until the reactor gives up waiting for it.
    if (!unpaired_moves.empty())
        return true;

    commitEvents();
    return false;
}

void DirectoryWatcherBase::onPairingTimeout()
{
    unpaired_moves.clear();
    commitEvents();
}

void DirectoryWatcherBase::onWatchRemoved()
{
    owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "Directory {} is not watched anymore: it was deleted or unmounted", path));
}

void DirectoryWatcherBase::commitEvents()
{
    owner.commitEvents();

    /// Wake up reader thread
    owner.storage.wakeUp();
}

#elif defined(OS_DARWIN)

int DirectoryWatcherBase::directoryFd() const
{
    return watch_state->dir_fd;
}

uint32_t DirectoryWatcherBase::directoryWatchFlags() const
{
    return NOTE_WRITE | NOTE_DELETE | NOTE_RENAME | NOTE_ATTRIB | NOTE_LINK;
}

void DirectoryWatcherBase::stopWatching()
{
    watch_state.reset();
}

std::optional<std::chrono::steady_clock::time_point> DirectoryWatcherBase::nextScan() const
{
    if (!watch_state)
        return std::nullopt;
    return watch_state->next_scan;
}

void DirectoryWatcherBase::onVnodeEvent(int fd, uint32_t fflags)
{
    /// `stopWatching` closes the descriptors, which removes them and their pending events from the kqueue.
    if (!watch_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The directory watcher of {} got an event of descriptor {} after it stopped watching", path, fd);

    const auto now = std::chrono::steady_clock::now();

    if (fd == watch_state->dir_fd)
    {
        /// An entry was added, removed or renamed, and only a scan finds out which: scan at once.
        if (fflags & (NOTE_WRITE | NOTE_LINK | NOTE_RENAME | NOTE_DELETE))
        {
            watch_state->directory_changed = true;
            watch_state->next_scan = now;
        }
        return;
    }

    auto name_it = watch_state->watched_names.find(fd);
    if (name_it == watch_state->watched_names.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The directory watcher of {} got an event of descriptor {}, which it does not watch", path, fd);

    /// The descriptor tells which file changed, so a change of its contents is reported at once, as inotify's
    /// IN_MODIFY is, and needs no scan. Truncation may come as NOTE_ATTRIB only.
    if ((fflags & (NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB)) && (eventMask() & DW_ITEM_MODIFIED))
    {
        owner.onItemModified(DirectoryEvent(name_it->second, DW_ITEM_MODIFIED));
        owner.commitEvents();
        owner.storage.wakeUp();
    }
    if (fflags & NOTE_RENAME)
    {
        watch_state->directory_changed = true;
        watch_state->next_scan = now;
    }
    if (fflags & NOTE_DELETE)
    {
        watch_state->next_scan = now;
        watch_state->deleted_files.insert(name_it->second);
        /// The name's identity ended; drop its now-stale fd so `syncFileWatches`
        /// reopens a fresh one if the name is recreated.
        watch_state->unwatchFile(watch_state->watched_fds.find(name_it->second));
    }
}

/// List the directory into `current_files_inodes` (name to inode). `syncFileWatches` holds a descriptor for every
/// file of the listing and `onTimer` compares it with the last snapshot, so it lists only the files the table may read:
/// - the regular files whose names match the globs of the path;
/// - the files of `followed_files` (the last snapshot, or the files the table read when the watcher was created)
///   under a name the globs exclude, e.g. a log renamed by rotation, which is read until it is removed. An excluded
///   file that is not followed, such as an archive compressed by `logrotate`, is a new inode and is skipped.
///
/// A followed file is listed under at most one excluded name, and under none when a matching name lists it.
/// Otherwise a hard link left next to a rotated log would list its inode under two names, and `onTimer` recognizes
/// a rename only when an inode has one name on each side, so the table would stop reading the rotated log. Of several
/// excluded names, the one in `followed_files` is kept, otherwise the smallest, so an unchanged name is not reported
/// as a rename.
///
/// A file that is created and renamed to an excluded name between two scans is never listed under a matching name,
/// so it is not read: in a listing, that rename looks the same as a new excluded file. The documentation of the
/// engine states this limitation.
void DirectoryWatcherBase::scan(std::map<std::string, UInt64> & current_files_inodes)
{
    const auto & followed_files = watch_state->followed_files;

    current_files_inodes.clear();
    /// The inode of each followed file that a name the globs exclude lists, to the name it is listed under.
    std::map<UInt64, std::string> followed_aliases;
    std::unordered_set<UInt64> matching_inodes;
    for (const auto & entry : std::filesystem::directory_iterator(path))
    {
        if (!entry.is_regular_file())
            continue;
        struct stat st{};
        if (::stat(entry.path().c_str(), &st) != 0)
            continue;
        std::string name = entry.path().filename().string();
        const auto inode = static_cast<UInt64>(st.st_ino);
        if (owner.storage.fileNameMatches(name))
        {
            matching_inodes.insert(inode);
            current_files_inodes.emplace(std::move(name), inode);
            continue;
        }
        auto followed_it = followed_files.find(inode);
        if (followed_it == followed_files.end())
            continue;
        auto [alias_it, inserted] = followed_aliases.try_emplace(inode, name);
        if (inserted)
            continue;
        /// Pick one alias deterministically: the followed name, otherwise the smallest name.
        const auto & chosen = alias_it->second;
        if (chosen != followed_it->second && (name == followed_it->second || name < chosen))
            alias_it->second = std::move(name);
    }
    for (auto & [inode, alias] : followed_aliases)
    {
        if (!matching_inodes.contains(inode))
            current_files_inodes.emplace(std::move(alias), inode);
    }
}

/// Keep the watches of the files in sync with the files currently present: each file is watched by opening it and
/// registering its descriptor in the kqueue of the reactor. A watch is keyed by name but pinned to a specific inode:
/// when a name is reused for a different inode (a rename plus recreate of the same name), the stale descriptor is
/// closed and a fresh one opened, so `NOTE_DELETE` is always attributed to the file that currently owns the name.
/// Returns after either watching every file in `files` or throwing; on ENOENT it drops the file
/// from `files` (see below). The caller runs this BEFORE emitting any events, so a throw here just
/// retries the whole pass with nothing queued - it can never strand events or kill the watcher.
void DirectoryWatcherBase::syncFileWatches(std::map<std::string, UInt64> & files)
{
    auto & watched_fds = watch_state->watched_fds;

    std::vector<std::string> vanished;
    for (const auto & [name, inode] : files)
    {
        if (auto it = watched_fds.find(name); it != watched_fds.end())
        {
            if (it->second.inode == inode)
                continue;
            /// Name now points to a different inode; drop the stale watch and reopen below.
            /// the kernel will remove the fd from kqueue's watch automatically when the fd is closed
            watch_state->unwatchFile(it);
        }
        int fd = ::open((std::filesystem::path(path) / name).c_str(), O_EVTONLY);
        if (fd == -1)
        {
            /// The file vanished between the scan and here (a delete, or a delete+recreate race
            /// like 04342). We cannot install a watch, so we must not keep it in the snapshot as a
            /// trusted same-inode entry - otherwise a same-inode recreate would be invisible and
            /// StorageFileLog would reuse a stale offset. Drop it: the next scan re-observes it as
            /// ADDED (re-read from 0), erring toward re-reading rather than silently skipping data.
            if (errno == ENOENT)
            {
                vanished.push_back(name);
                continue;
            }
            /// Any other failure (e.g. EMFILE under fd pressure) is transient. Throw so the caller
            /// retries the whole pass; do NOT surface it via owner.onError, which would leave a
            /// stale error to be reported once ingestion recovers.
            ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "Cannot open file to watch");
        }
        try
        {
            reactor->watchVnode(*this, fd, NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB | NOTE_DELETE | NOTE_RENAME);
        }
        catch (...)
        {
            closeFileDescriptor(fd);
            throw;
        }
        watch_state->watchFile(name, fd, inode);
    }
    for (const auto & name : vanished)
        files.erase(name);
    for (auto it = watched_fds.begin(); it != watched_fds.end();)
    {
        if (files.contains(it->first))
        {
            ++it;
            continue;
        }
        it = watch_state->unwatchFile(it);
    }
}

/// Listing, `stat` and opening the watches are not atomic with a rename, which would be diffed as
/// REMOVED + ADDED, so the caller discards a pass the directory changed under. The events are routed to
/// `onVnodeEvent` before `syncFileWatches` to keep the NOTE_DELETE of a departed name, which closing its watch drops.
bool DirectoryWatcherBase::scanFilesAndUpdateWatches(std::map<std::string, UInt64> & current_files_inodes)
{
    /// The events that came before are covered by the scan below.
    watch_state->directory_changed = false;
    scan(current_files_inodes);
    /// Learn whether the directory changed during the scan, and take the `NOTE_DELETE` of the departed files
    /// before `syncFileWatches` closes their descriptors, which drops the events not read yet.
    reactor->drainReadyEvents();
    syncFileWatches(current_files_inodes);
    /// did syncFileWatches close file descriptors before their events were read?
    reactor->drainReadyEvents();
    return std::exchange(watch_state->directory_changed, false);
}

void DirectoryWatcherBase::onTimer()
{
    const auto now = std::chrono::steady_clock::now();

    /// Pre-existing files are loaded by StorageFileLog's own directory scan; the watcher, like
    /// inotify, reports only subsequent changes. So the first snapshot is taken without emitting events. A
    /// transient failure here (e.g. the directory being briefly recreated) must not permanently kill
    /// the watcher, so we log and retry rather than give up - StorageFileLog::loadFiles has
    /// already captured the initial file set, and retrying keeps the baseline correct (starting from an
    /// empty snapshot would re-emit every pre-existing file as ADDED and re-read it from offset 0).
    std::map<std::string, UInt64> current_files_inodes;
    if (!watch_state->has_snapshot)
    {
        try
        {
            if (scanFilesAndUpdateWatches(current_files_inodes))
            {
                watch_state->next_scan = now;
                return;
            }
            watch_state->setSnapshot(std::move(current_files_inodes));
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(getLogger("FileLogDirectoryWatcher"), "Failed to take the first snapshot of watched directory {}, will retry: {}", path, e.what());
        }
        watch_state->next_scan = now + std::chrono::milliseconds(watch_state->milliseconds_to_wait);
        return;
    }

    const auto & settings = owner.storage.getFileLogSettings();
    auto & snapshot = watch_state->snapshot;
    auto & deleted_files = watch_state->deleted_files;

    try
    {
        /// Install/refresh the per-file watches for the new set BEFORE emitting any events. A
        /// transient failure here (e.g. EMFILE) then just retries the whole pass with nothing
        /// queued and StorageFileLog left untouched, instead of stranding a half-emitted batch
        /// behind a dead watcher. It also drops any file that vanished mid-scan from `current_files_inodes`.
        if (scanFilesAndUpdateWatches(current_files_inodes))
        {
            watch_state->next_scan = now;
            return;
        }
    }
    catch (const std::exception & e)
    {
        /// The directory can be transiently unavailable (e.g. while being recreated) and watch
        /// installation can transiently fail; neither is expected during normal operation, so log
        /// it and retry on the next tick. The `deleted_files` set collected so far is preserved for that retry.
        LOG_WARNING(getLogger("FileLogDirectoryWatcher"), "Failed to scan/watch directory {}, will retry: {}", path, e.what());
        watch_state->next_scan = now + std::chrono::milliseconds(watch_state->milliseconds_to_wait);
        return;
    }

    bool changed = false;

    /// Track identities by inode so a rename is recognized even when the source name is
    /// recreated in the same scan window (e.g. `a -> b` plus a fresh `a`): the old inode still
    /// left name `a` for name `b`, which must stay a MOVED pair (preserving the read offset)
    /// rather than degenerating into two ADDED events. An inode is only trusted for move
    /// detection when it maps to exactly one name on both sides (guards against hard links).
    std::map<UInt64, size_t> snapshot_inode_count;
    std::map<UInt64, size_t> current_inode_count;
    std::map<UInt64, std::string> snapshot_inode_name;
    std::map<UInt64, std::string> current_inode_name;
    for (const auto & [name, inode] : snapshot)
    {
        ++snapshot_inode_count[inode];
        snapshot_inode_name[inode] = name;
    }
    for (const auto & [name, inode] : current_files_inodes)
    {
        ++current_inode_count[inode];
        current_inode_name[inode] = name;
    }

    /// True if `inode` uniquely identifies one name on each side and those names differ, i.e. it
    /// is a rename of that inode from its old name to its new name.
    auto is_rename = [&](UInt64 inode)
    {
        auto sc = snapshot_inode_count.find(inode);
        auto cc = current_inode_count.find(inode);
        if (sc == snapshot_inode_count.end() || cc == current_inode_count.end() || sc->second != 1 || cc->second != 1)
            return false;
        /// An unlinked name is not a rename source, even when a hard link brings its inode back.
        if (deleted_files.contains(snapshot_inode_name.at(inode)))
            return false;
        return snapshot_inode_name.at(inode) != current_inode_name.at(inode);
    };

    /// A name keeps its identity if it exists on both sides with the same inode and was not
    /// unlinked in between (a delete+recreate reusing the inode breaks the identity).
    auto same_identity = [&deleted_files](const std::map<std::string, UInt64> & other, const std::string & name, UInt64 inode)
    {
        if (deleted_files.contains(name))
            return false;
        auto it = other.find(name);
        return it != other.end() && it->second == inode;
    };

    /// Order the rename targets (MOVED_TO). A target whose OLD inode is itself being moved
    /// elsewhere must be emitted AFTER the MOVED_TO that relocates that old inode, otherwise
    /// reusing the name erases the still-live inode's meta and it is re-read from offset 0 (e.g.
    /// a logrotate chain `a.1 -> a.2`, `a -> a.1`, new `a`). A topological sort over "target T
    /// follows the target that receives T's old inode" recovers that chronological order.
    ///
    /// The only unorderable shape is a rename cycle (e.g. an `a <-> b` swap coalesced into one
    /// scan): no order preserves every offset, and unlike inotify we cannot observe the
    /// intermediate temporary-name events that would break it. We cannot do better from a
    /// directory snapshot, so, like the Linux inotify watcher which applies whatever events it
    /// receives, we emit the leftovers best-effort; StorageFileLog then re-reads one rotated file.
    std::vector<std::string> move_targets;
    for (const auto & [name, inode] : current_files_inodes)
    {
        if (!same_identity(snapshot, name, inode) && is_rename(inode))
            move_targets.push_back(name);
    }

    std::vector<std::string> ordered_moves;
    {
        const std::set<std::string> target_set(move_targets.begin(), move_targets.end());
        std::map<std::string, std::string> predecessor;
        for (const auto & name : move_targets)
        {
            auto snap_it = snapshot.find(name);
            if (snap_it == snapshot.end())
                continue;
            const UInt64 old_inode = snap_it->second;
            auto relocated_it = current_inode_name.find(old_inode);
            if (is_rename(old_inode) && relocated_it != current_inode_name.end()
                && relocated_it->second != name && target_set.contains(relocated_it->second))
                predecessor[name] = relocated_it->second;
        }

        std::set<std::string> ordered_set;
        bool progress = true;
        while (ordered_moves.size() < move_targets.size() && progress)
        {
            progress = false;
            for (const auto & name : move_targets)
            {
                if (ordered_set.contains(name))
                    continue;
                auto p = predecessor.find(name);
                if (p == predecessor.end() || ordered_set.contains(p->second))
                {
                    ordered_moves.push_back(name);
                    ordered_set.insert(name);
                    progress = true;
                }
            }
        }

        /// Leftovers form a rename cycle; append them in a deterministic order (best effort).
        for (const auto & name : move_targets)
        {
            if (!ordered_set.contains(name))
                ordered_moves.push_back(name);
        }
    }

    /// Emit in the order StorageFileLog expects: MOVED_FROM/REMOVED, then MOVED_TO, then ADDED.
    /// In particular MOVED_TO precedes the ADDED of a recreated source name so the
    /// moved inode's meta is renamed before the fresh file resets it.

    /// Departed identities (name gone, or its inode replaced at that name).
    for (const auto & [name, inode] : snapshot)
    {
        if (same_identity(current_files_inodes, name, inode))
            continue;
        changed = true;
        if (is_rename(inode))
        {
            if (eventMask() & DW_ITEM_MOVED_FROM)
                owner.onItemMovedFrom(DirectoryEvent(name, DW_ITEM_MOVED_FROM, inode));
        }
        else if (eventMask() & DW_ITEM_REMOVED)
            owner.onItemRemoved(DirectoryEvent(name, DW_ITEM_REMOVED));
    }

    /// Arrived rename targets, in the topologically-sorted order computed above.
    for (const auto & name : ordered_moves)
    {
        changed = true;
        if (eventMask() & DW_ITEM_MOVED_TO)
            owner.onItemMovedTo(DirectoryEvent(name, DW_ITEM_MOVED_TO, current_files_inodes.at(name)));
    }

    /// Arrived identities that are genuinely new (including a name whose inode was replaced).
    for (const auto & [name, inode] : current_files_inodes)
    {
        if (same_identity(snapshot, name, inode) || is_rename(inode))
            continue;
        changed = true;
        if (eventMask() & DW_ITEM_ADDED)
            owner.onItemAdded(DirectoryEvent(name, DW_ITEM_ADDED));
    }

    owner.commitEvents();
    watch_state->setSnapshot(std::move(current_files_inodes));
    /// This pass committed successfully, so the deletes it collected have been applied; start the next
    /// pass with a clean set. (On a retried pass we return early, keeping them.)
    deleted_files.clear();

    if (changed)
    {
        watch_state->milliseconds_to_wait = (*settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();
        owner.storage.wakeUp();
    }
    else if (watch_state->milliseconds_to_wait < static_cast<uint64_t>((*settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds()))
    {
        watch_state->milliseconds_to_wait *= (*settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value;
    }

    watch_state->next_scan = now + std::chrono::milliseconds(watch_state->milliseconds_to_wait);
}

#endif

DirectoryWatcherBase::~DirectoryWatcherBase()
{
    if (registered)
    {
        /// Returns after the reactor thread has stopped using this watcher and closed its descriptors, so the members
        /// can be destroyed.
        try
        {
            reactor->remove(*this);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
