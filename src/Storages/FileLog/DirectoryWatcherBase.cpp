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
#include <algorithm>
#include <map>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/param.h>
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
        /// macOS has no inotify: events are rebuilt by diffing a name -> inode snapshot, so a rename keeps its offset.
        /// Directory events trigger a scan, per-file descriptors report `DW_ITEM_MODIFIED` (deferred to the scan while a
        /// structural change is pending). A timer retries failed scans and, with backoff, catches network FS changes.
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
        has_uncommitted_events = true;
        owner.onItemAdded(ev);
    }
    if ((p_event->mask & IN_DELETE) && (eventMask() & DirectoryWatcherBase::DW_ITEM_REMOVED))
    {
        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_REMOVED);
        has_uncommitted_events = true;
        owner.onItemRemoved(ev);
    }
    if ((p_event->mask & IN_MODIFY) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MODIFIED))
    {
        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MODIFIED);
        has_uncommitted_events = true;
        owner.onItemModified(ev);
    }
    if ((p_event->mask & IN_MOVED_FROM) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_FROM))
    {
        unpaired_move_cookies.push_back(p_event->cookie);
        if (unpaired_move_cookies.size() > max_unpaired_move_cookies)
            unpaired_move_cookies.pop_front();

        /// Started when the watcher begins to wait and not extended by later renames, so that they cannot postpone the
        /// commit. A late `IN_MOVED_TO` is still paired by `unpaired_move_cookies`.
        if (unpaired_moves.empty())
            pairing_deadline = std::chrono::steady_clock::now() + pairing_timeout;
        unpaired_moves.insert(p_event->cookie);

        DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_FROM, p_event->cookie);
        has_uncommitted_events = true;
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
                has_uncommitted_events = true;
                owner.onItemMovedTo(ev);
            }
        }
        else if (eventMask() & DirectoryWatcherBase::DW_ITEM_ADDED)
        {
            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_ADDED);
            has_uncommitted_events = true;
            owner.onItemAdded(ev);
        }
    }

    if (p_event->mask & IN_MOVED_TO)
        unpaired_moves.erase(p_event->cookie);
}

bool DirectoryWatcherBase::hasUnpairedMoves() const
{
    /// The IN_MOVED_FROM and IN_MOVED_TO of one rename can come in two reads (see inotify(7)): the events are held
    /// back until every IN_MOVED_FROM has its IN_MOVED_TO, or until the reactor gives up waiting for it.
    return !unpaired_moves.empty();
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
            watch_state->structural_change_pending = true;
            watch_state->next_scan = now;
        }
        return;
    }

    auto name_it = watch_state->watched_names.find(fd);
    if (name_it == watch_state->watched_names.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The directory watcher of {} got an event of descriptor {}, which it does not watch", path, fd);

    /// Before the modification below: kqueue merges the flags of the events not read yet, so a rename and a write
    /// after it can come in one event.
    if (fflags & (NOTE_RENAME | NOTE_DELETE))
        watch_state->structural_change_pending = true;

    /// The descriptor tells which file changed, so a change of its contents is reported at once, as inotify's
    /// IN_MODIFY is, and needs no scan. Truncation may come as NOTE_ATTRIB only. While a structural change is pending,
    /// the name of the descriptor may belong to another file now, so the modification is left to the scan.
    if ((fflags & (NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB)) && (eventMask() & DW_ITEM_MODIFIED))
    {
        if (watch_state->structural_change_pending)
        {
            watch_state->deferred_modifications.insert(watch_state->watched_fds.at(name_it->second).inode);
        }
        else
        {
            owner.onItemModified(DirectoryEvent(name_it->second, DW_ITEM_MODIFIED));
            has_uncommitted_events = true;
        }
    }
    if (fflags & NOTE_RENAME)
    {
        watch_state->directory_changed = true;
        watch_state->next_scan = now;
    }
    if (fflags & NOTE_DELETE)
    {
        watch_state->next_scan = now;
        /// An unlink of any name is reported to every descriptor of the file. While links are left, only a name went
        /// away and the scan finds which one. The descriptor keeps the vnode, so a recreate reusing the inode sees 0.
        struct stat st{};
        if (::fstat(fd, &st) != 0)
            ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "Cannot stat watched file {}", name_it->second);
        if (st.st_nlink > 0)
        {
            watch_state->directory_changed = true;
            return;
        }
        watch_state->deleted_files.insert(name_it->second);
        /// The name's identity ended; drop its now-stale fd so `syncFileWatches`
        /// reopens a fresh one if the name is recreated.
        watch_state->unwatchFile(watch_state->watched_fds.find(name_it->second));
    }
}


void DirectoryWatcherBase::scan(std::map<std::string, UInt64> & current_files_inodes)
{
    const auto & followed_files = watch_state->followed_files;

    current_files_inodes.clear();
    /// The inode of each followed file that names the globs exclude list, to those names.
    std::map<UInt64, std::vector<std::string>> followed_aliases;
    std::unordered_set<UInt64> matching_inodes;
    for (const auto & entry : std::filesystem::directory_iterator(path))
    {
        if (!entry.is_regular_file())
            continue;
        struct stat st{};
        if (::stat(entry.path().c_str(), &st) != 0)
            //failure is expected in some cases like rename
            continue;
        std::string file_name = entry.path().filename().string();
        const auto inode = static_cast<UInt64>(st.st_ino);
        if (owner.storage.fileNameMatches(file_name))
        {
            matching_inodes.insert(inode);
            current_files_inodes.emplace(std::move(file_name), inode);
            continue;
        }
        if (followed_files.contains(inode))
            followed_aliases[inode].push_back(std::move(file_name));
    }
    for (auto & [inode, aliases] : followed_aliases)
    {
        if (!matching_inodes.contains(inode))
            current_files_inodes.emplace(chooseFollowedAlias(inode, aliases), inode);
    }
}

/// Pick one alias deterministically: the followed name, otherwise the name the file was renamed to, otherwise the
/// smallest name.
std::string DirectoryWatcherBase::chooseFollowedAlias(UInt64 inode, std::vector<std::string> & aliases) const
{
    if (aliases.size() == 1)
        return std::move(aliases.front());

    const auto & followed_name = watch_state->followed_files.at(inode);
    if (std::ranges::find(aliases, followed_name) != aliases.end())
        return followed_name;

    /// The kernel renames the vnode with the file, so its descriptor has the name the followed file was renamed to,
    /// e.g. a rotated log with a hard link left next to it. The name is not known after an unlink of another name.
    if (auto watched_it = watch_state->watched_fds.find(followed_name);
        watched_it != watch_state->watched_fds.end() && watched_it->second.inode == inode)
    {
        char vnode_path[MAXPATHLEN];
        if (::fcntl(watched_it->second.fd, F_GETPATH, vnode_path) != -1)
        {
            std::string vnode_name = std::filesystem::path(vnode_path).filename().string();
            if (std::ranges::find(aliases, vnode_name) != aliases.end())
                return vnode_name;
        }
    }

    return std::move(*std::ranges::min_element(aliases));
}

/// Make the kqueue watch exactly the files in `files` (name -> inode). A name whose inode changed is re-opened.
/// Files that vanished before they could be opened are removed from `files`. Called before any events are emitted,
/// so a throw only causes the pass to be retried.
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
            /// The file vanished since the scan. Without a watch a same-inode recreate would be invisible and reuse a
            /// stale offset, so drop it: the next scan reports it as ADDED, re-reading rather than skipping data.
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
            /// Until it is recorded, the descriptor is owned here: closing it removes it from the kqueue, which
            /// otherwise could return the registration after the watcher is removed.
            watch_state->watchFile(name, fd, inode);
        }
        catch (...)
        {
            closeFileDescriptor(fd);
            throw;
        }
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

/// Returns true if the pass must be discarded and retried: the directory changed under it (a rename would be diffed
/// as REMOVED + ADDED), or a drain did not take all ready events. Events are drained before `syncFileWatches`
/// to keep the `NOTE_DELETE` of departed names, which closing their watches drops.
bool DirectoryWatcherBase::scanFilesAndUpdateWatches(std::map<std::string, UInt64> & current_files_inodes)
{
    /// The events that came before are covered by the scan below.
    watch_state->directory_changed = false;
    scan(current_files_inodes);
    /// Learn whether the directory changed during the scan, and take the `NOTE_DELETE` of the departed files
    /// before `syncFileWatches` closes their descriptors, which drops the events not read yet.
    if (!reactor->drainReadyEvents())
        return true;
    syncFileWatches(current_files_inodes);
    /// The directory may have changed between the listing and the opens of `syncFileWatches`, which would pin a watch
    /// to another inode than the listed one. The events of the files whose descriptors it closed are dropped, but the
    /// directory reports the change too, so discard the pass if it changed.
    if (!reactor->drainReadyEvents())
        return true;
    return std::exchange(watch_state->directory_changed, false);
}

bool DirectoryWatcherBase::emitDeferredModifications(const std::map<std::string, UInt64> & current_files_inodes)
{
    auto & deferred_modifications = watch_state->deferred_modifications;
    if (deferred_modifications.empty())
        return false;

    /// A file that is not listed anymore was removed or moved out, which the events of the scan report.
    bool emitted = false;
    for (const auto & [name, inode] : current_files_inodes)
    {
        if (!deferred_modifications.contains(inode))
            continue;
        emitted = true;
        owner.onItemModified(DirectoryEvent(name, DW_ITEM_MODIFIED));
    }
    deferred_modifications.clear();
    return emitted;
}

void DirectoryWatcherBase::onTimer()
{
    const auto now = std::chrono::steady_clock::now();

    /// Pre-existing files are loaded by `StorageFileLog::loadFiles`, so like inotify the first snapshot emits no events.
    /// A transient failure is retried: an empty baseline would re-emit every pre-existing file as ADDED.
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
            /// The baseline emits no events of the listing, but the files it watches may have been modified since.
            if (emitDeferredModifications(current_files_inodes))
                has_uncommitted_events = true;
            watch_state->structural_change_pending = false;
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
        /// Refresh the per-file watches BEFORE emitting any events, so a transient failure (e.g. `EMFILE`) retries
        /// the pass with nothing half-emitted. It also drops files that vanished mid-scan from `current_files_inodes`.
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

    /// Track identities by inode so a rename is recognized even when the source is recreated in the same window
    /// (`a -> b` plus a fresh `a` stays a MOVED pair, preserving the offset). An inode is trusted for move detection
    /// only when it maps to exactly one name on both sides (guards against hard links).
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

    /// Topologically order the MOVED_TO targets so a target follows the move of its old inode, otherwise its meta is
    /// erased and re-read from 0 (e.g. logrotate `a.1 -> a.2`, `a -> a.1`, new `a`). A rename cycle (an `a <-> b`
    /// swap in one scan) cannot be ordered from a snapshot, so it is emitted best-effort and one file is re-read.
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
        {
            owner.onItemRemoved(DirectoryEvent(name, DW_ITEM_REMOVED));
        }
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

    /// The modifications deferred while the change was pending, under the names of this listing and after the events
    /// that renamed or replaced the files, in the same batch.
    if (emitDeferredModifications(current_files_inodes))
        changed = true;

    watch_state->setSnapshot(std::move(current_files_inodes));
    /// This pass committed successfully, so the deletes it collected have been applied; start the next
    /// pass with a clean set. (On a retried pass we return early, keeping them.)
    deleted_files.clear();
    watch_state->structural_change_pending = false;

    if (changed)
    {
        /// The reactor commits the events once the due scans are done.
        has_uncommitted_events = true;
        watch_state->milliseconds_to_wait = (*settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();
    }
    else if (watch_state->milliseconds_to_wait < static_cast<uint64_t>((*settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds()))
    {
        watch_state->milliseconds_to_wait *= (*settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value;
    }

    watch_state->next_scan = now + std::chrono::milliseconds(watch_state->milliseconds_to_wait);
}

#endif

void DirectoryWatcherBase::commitEvents()
{
    if (!has_uncommitted_events)
    {
        return;
    }
    owner.commitEvents();
    has_uncommitted_events = false;

    /// Wake up reader thread
    owner.storage.wakeUp();
}

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
