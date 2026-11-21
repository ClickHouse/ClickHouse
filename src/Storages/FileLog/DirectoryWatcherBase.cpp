#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Storages/FileLog/DirectoryWatcherBase.h>
#include <Storages/FileLog/FileLogDirectoryWatcher.h>
#include <Storages/FileLog/FileLogSettings.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <base/defines.h>

#include <filesystem>
#include <unistd.h>
#include <poll.h>
#include <Common/ErrnoException.h>
#include <Common/logger_useful.h>

#if defined(OS_LINUX)
#include <sys/inotify.h>
#elif defined(OS_DARWIN)
#include <map>
#include <set>
#include <vector>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/stat.h>
#include <base/scope_guard.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int IO_SETUP_ERROR;
}

namespace FileLogSetting
{
    extern const FileLogSettingsUInt64 poll_directory_watch_events_backoff_factor;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_init;
    extern const FileLogSettingsMilliseconds poll_directory_watch_events_backoff_max;
}

#if defined(OS_LINUX)
static constexpr int buffer_size = 4096;
#endif

DirectoryWatcherBase::DirectoryWatcherBase(
    FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_)
    : WithContext(context_)
    , owner(owner_)
    , path(path_)
    , event_mask(event_mask_)
    , milliseconds_to_wait((*owner.storage.getFileLogSettings())[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds())
{
    if (!std::filesystem::exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Path {} does not exist", path);

    if (!std::filesystem::is_directory(path))
        throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path {} is not a directory", path);

#if defined(OS_LINUX)
    inotify_fd = inotify_init();
    if (inotify_fd == -1)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot initialize inotify");
#endif

    watch_task = getContext()->getSchedulePool().createTask(StorageID::createEmpty(), "directory_watch", [this] { watchFunc(); });
    start();
}

#if defined(OS_LINUX)
void DirectoryWatcherBase::watchFunc()
{
    int mask = 0;
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

    int wd = inotify_add_watch(inotify_fd, path.c_str(), mask);
    if (wd == -1)
    {
        owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "Watch directory {} failed", path));
        ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "Watch directory {} failed", path);
    }

    std::string buffer;
    buffer.resize(buffer_size);
    pollfd pfds[2];
    /// inotify descriptor
    pfds[0].fd = inotify_fd;
    pfds[0].events = POLLIN;
    // notifier
    pfds[1].fd = event_pipe.fds_rw[0];
    pfds[1].events = POLLIN;
    while (!stopped)
    {
        const auto & settings = owner.storage.getFileLogSettings();
        if (poll(pfds, 2, static_cast<int>(milliseconds_to_wait)) > 0 && pfds[0].revents & POLLIN)
        {
            milliseconds_to_wait = (*settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();
            ssize_t n = read(inotify_fd, buffer.data(), buffer.size());
            int i = 0;
            if (n > 0)
            {
                while (n > 0)
                {
                    struct inotify_event * p_event = reinterpret_cast<struct inotify_event *>(buffer.data() + i);

                    if (p_event->len > 0)
                    {
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
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_FROM);
                            owner.onItemMovedFrom(ev);
                        }
                        if ((p_event->mask & IN_MOVED_TO) && (eventMask() & DirectoryWatcherBase::DW_ITEM_MOVED_TO))
                        {
                            DirectoryWatcherBase::DirectoryEvent ev(p_event->name, DirectoryWatcherBase::DW_ITEM_MOVED_TO);
                            owner.onItemMovedTo(ev);
                        }
                    }

                    i += sizeof(inotify_event) + p_event->len;
                    n -= sizeof(inotify_event) + p_event->len;
                }
            }

            /// Wake up reader thread
            owner.storage.wakeUp();
        }
        else
        {
            if (milliseconds_to_wait < static_cast<uint64_t>((*settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds()))
                milliseconds_to_wait *= (*settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value;
        }
    }
}
#elif defined(OS_DARWIN)
static void closeFileDescriptor(int fd)
{
    [[maybe_unused]] int err = ::close(fd);
    chassert(!err || errno == EINTR);
}

void DirectoryWatcherBase::watchFunc()
{
    /// macOS has no inotify. We reconstruct the same event stream by diffing a filename ->
    /// (inode, mtime, size) snapshot of the directory. Tracking the inode lets us tell a rename
    /// (same inode reappearing under a new name) apart from a delete followed by a create, so the
    /// read offset of a renamed file is preserved just like with inotify's IN_MOVED_FROM/IN_MOVED_TO.
    ///
    /// kqueue reports "the directory changed" (an entry added/removed/renamed) but neither names the
    /// entry nor fires on appends to files already inside it. So we use it only to wake up promptly
    /// on structural changes (matching inotify's latency, which the no-sleep test sequences rely on),
    /// and fall back to the timed rescan to pick up appends to existing files.
    struct FileState
    {
        UInt64 inode;
        Int64 mtime_ns;
        Int64 size;
    };

    auto scan = [this](std::map<std::string, FileState> & out)
    {
        out.clear();
        for (const auto & entry : std::filesystem::directory_iterator(path))
        {
            if (!entry.is_regular_file())
                continue;
            struct stat st{};
            if (::stat(entry.path().c_str(), &st) != 0)
                continue;
            out.emplace(
                entry.path().filename().string(),
                FileState{
                    static_cast<UInt64>(st.st_ino),
                    static_cast<Int64>(st.st_mtimespec.tv_sec) * 1'000'000'000 + st.st_mtimespec.tv_nsec,
                    static_cast<Int64>(st.st_size)});
        }
    };

    /// A kqueue wakes the loop promptly. The directory fd fires on structural changes (an entry
    /// added/removed/renamed) but not on appends/truncations to files already inside it, so we also
    /// watch each file's own fd for content changes and for the NOTE_DELETE that a snapshot diff
    /// needs to tell a same-inode delete+recreate from a plain modification. Because that per-file
    /// NOTE_DELETE is load-bearing, kqueue is required: we fail closed on any setup error (mirroring
    /// the Linux watcher, which throws when inotify_init/inotify_add_watch fail) rather than degrade
    /// to a polling mode that would silently keep stale read offsets.
    struct WatchedFd
    {
        int fd;
        UInt64 inode;
    };
    std::map<std::string, WatchedFd> watched_fds;
    int dir_fd = ::open(path.c_str(), O_EVTONLY);
    int kq = dir_fd == -1 ? -1 : kqueue();

    SCOPE_EXIT({
        for (const auto & [name, watched] : watched_fds)
            closeFileDescriptor(watched.fd);
        if (kq != -1)
            closeFileDescriptor(kq);
        if (dir_fd != -1)
            closeFileDescriptor(dir_fd);
    });

    auto fail = [&](std::string_view what)
    {
        owner.onError(Exception(ErrorCodes::IO_SETUP_ERROR, "{}: {}", what, path));
        ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "{}", what);
    };

    if (dir_fd == -1)
        fail("Cannot open directory to watch");
    if (kq == -1)
        fail("Cannot create kqueue to watch directory");

    {
        struct kevent change{};
        EV_SET(&change, dir_fd, EVFILT_VNODE, EV_ADD | EV_CLEAR,
               NOTE_WRITE | NOTE_DELETE | NOTE_RENAME | NOTE_ATTRIB | NOTE_LINK, 0, nullptr);
        if (kevent(kq, &change, 1, nullptr, 0, nullptr) == -1)
            fail("Cannot register directory watch");
    }

    /// Keep the per-file kqueue watches in sync with the files currently present. A watch is keyed by
    /// name but pinned to a specific inode: when a name is reused for a different inode (a rename plus
    /// recreate of the same name), the stale fd is closed and a fresh one opened, so NOTE_DELETE is
    /// always attributed to the file that currently owns the name.
    /// Returns after either watching every file in `files` or throwing; on ENOENT it drops the file
    /// from `files` (see below). The caller runs this BEFORE emitting any events, so a throw here just
    /// retries the whole pass with nothing queued - it can never strand events or kill the watcher.
    auto sync_file_watches = [&](std::map<std::string, FileState> & files)
    {
        std::vector<std::string> vanished;
        for (const auto & [name, state] : files)
        {
            if (auto it = watched_fds.find(name); it != watched_fds.end())
            {
                if (it->second.inode == state.inode)
                    continue;
                /// Name now points to a different inode; drop the stale watch and reopen below.
                closeFileDescriptor(it->second.fd);
                watched_fds.erase(it);
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
            struct kevent change{};
            EV_SET(&change, fd, EVFILT_VNODE, EV_ADD | EV_CLEAR,
                   NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB | NOTE_DELETE | NOTE_RENAME, 0, nullptr);
            if (kevent(kq, &change, 1, nullptr, 0, nullptr) == -1)
            {
                const int saved_errno = errno;
                closeFileDescriptor(fd);
                errno = saved_errno;
                ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, path, "Cannot register file watch");
            }
            watched_fds.emplace(name, WatchedFd{fd, state.inode});
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
            closeFileDescriptor(it->second.fd); /// closing the fd removes it from the kqueue
            it = watched_fds.erase(it);
        }
    };

    /// Pre-existing files are loaded by StorageFileLog's own directory scan; the watcher, like
    /// inotify, reports only subsequent changes. So seed the snapshot without emitting events. A
    /// transient failure here (e.g. the directory being briefly recreated) must not permanently kill
    /// this one-shot task, so we log and retry rather than return - StorageFileLog::loadFiles has
    /// already captured the initial file set, and retrying keeps the baseline correct (seeding from an
    /// empty snapshot would re-emit every pre-existing file as ADDED and re-read it from offset 0).
    std::map<std::string, FileState> snapshot;
    while (!stopped)
    {
        try
        {
            scan(snapshot);
            sync_file_watches(snapshot);
            break;
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(getLogger("FileLogDirectoryWatcher"), "Failed to seed watched directory {}, will retry: {}", path, e.what());
            pollfd stop_wait{.fd = event_pipe.fds_rw[0], .events = POLLIN, .revents = 0};
            poll(&stop_wait, 1, static_cast<int>(milliseconds_to_wait)); /// interruptible wait before retrying
        }
    }
    if (stopped)
        return;

    pollfd pfds[2];
    pfds[0].fd = event_pipe.fds_rw[0];
    pfds[0].events = POLLIN;
    pfds[1].fd = kq;
    pfds[1].events = POLLIN;

    /// Names whose watched inode was unlinked (NOTE_DELETE), accumulated across drains. Kept outside
    /// the loop so a pass that is retried (e.g. a transient scan/watch failure) does not lose the
    /// deletes it already drained - EV_CLEAR makes the kqueue events edge-triggered, so a dropped
    /// NOTE_DELETE would never be re-reported and a same-inode recreate would slip through as MODIFIED.
    std::set<std::string> deleted;
    while (!stopped)
    {
        if (poll(pfds, 2, static_cast<int>(milliseconds_to_wait)) < 0 && errno != EINTR)
            break;
        if (stopped)
            break;

        /// Drain kqueue notifications (level-triggered otherwise) before rescanning, noting which
        /// watched files were unlinked. A same-name delete+recreate that reuses the inode looks
        /// identical to a plain modification in a snapshot diff, so we rely on the per-file
        /// NOTE_DELETE to force an identity reset (REMOVED + ADDED) instead of MODIFIED, which would
        /// otherwise keep a stale read offset - matching what inotify's IN_DELETE + IN_CREATE gives.
        if (pfds[1].revents & POLLIN)
        {
            struct kevent evs[16];
            struct timespec no_wait{0, 0};
            int drained = 0;
            while ((drained = kevent(kq, nullptr, 0, evs, 16, &no_wait)) > 0)
            {
                for (int i = 0; i < drained; ++i)
                {
                    if (!(evs[i].fflags & NOTE_DELETE))
                        continue;
                    const int event_fd = static_cast<int>(evs[i].ident);
                    for (auto it = watched_fds.begin(); it != watched_fds.end(); ++it)
                    {
                        if (it->second.fd != event_fd)
                            continue;
                        deleted.insert(it->first);
                        /// The name's identity ended; drop its now-stale fd so sync_file_watches
                        /// reopens a fresh one if the name is recreated.
                        closeFileDescriptor(it->second.fd);
                        watched_fds.erase(it);
                        break;
                    }
                }
            }
        }

        const auto & settings = owner.storage.getFileLogSettings();

        std::map<std::string, FileState> current;
        try
        {
            scan(current);
            /// Install/refresh the per-file watches for the new set BEFORE emitting any events. A
            /// transient failure here (e.g. EMFILE) then just retries the whole pass with nothing
            /// queued and StorageFileLog left untouched, instead of stranding a half-emitted batch
            /// behind a dead watcher. It also drops any file that vanished mid-scan from `current`.
            sync_file_watches(current);
        }
        catch (const std::exception & e)
        {
            /// The directory can be transiently unavailable (e.g. while being recreated) and watch
            /// installation can transiently fail; neither is expected during normal operation, so log
            /// it and retry on the next tick. The drained `deleted` set is preserved for that retry.
            LOG_WARNING(getLogger("FileLogDirectoryWatcher"), "Failed to scan/watch directory {}, will retry: {}", path, e.what());
            continue;
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
        for (const auto & [name, state] : snapshot)
        {
            ++snapshot_inode_count[state.inode];
            snapshot_inode_name[state.inode] = name;
        }
        for (const auto & [name, state] : current)
        {
            ++current_inode_count[state.inode];
            current_inode_name[state.inode] = name;
        }

        /// True if `inode` uniquely identifies one name on each side and those names differ, i.e. it
        /// is a rename of that inode from its old name to its new name.
        auto is_rename = [&](UInt64 inode)
        {
            auto sc = snapshot_inode_count.find(inode);
            auto cc = current_inode_count.find(inode);
            if (sc == snapshot_inode_count.end() || cc == current_inode_count.end() || sc->second != 1 || cc->second != 1)
                return false;
            return snapshot_inode_name.at(inode) != current_inode_name.at(inode);
        };

        /// A name keeps its identity if it exists on both sides with the same inode and was not
        /// unlinked in between (a delete+recreate reusing the inode breaks the identity).
        auto same_identity = [&deleted](const std::map<std::string, FileState> & other, const std::string & name, UInt64 inode)
        {
            if (deleted.contains(name))
                return false;
            auto it = other.find(name);
            return it != other.end() && it->second.inode == inode;
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
        for (const auto & [name, state] : current)
        {
            if (!same_identity(snapshot, name, state.inode) && is_rename(state.inode))
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
                const UInt64 old_inode = snap_it->second.inode;
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

        /// Emit in the order StorageFileLog expects: MOVED_FROM/REMOVED, then MOVED_TO, then ADDED,
        /// then MODIFIED. In particular MOVED_TO precedes the ADDED of a recreated source name so the
        /// moved inode's meta is renamed before the fresh file resets it.

        /// Departed identities (name gone, or its inode replaced at that name).
        for (const auto & [name, state] : snapshot)
        {
            if (same_identity(current, name, state.inode))
                continue;
            changed = true;
            if (is_rename(state.inode))
            {
                if (eventMask() & DW_ITEM_MOVED_FROM)
                    owner.onItemMovedFrom(DirectoryEvent(name, DW_ITEM_MOVED_FROM));
            }
            else if (eventMask() & DW_ITEM_REMOVED)
                owner.onItemRemoved(DirectoryEvent(name, DW_ITEM_REMOVED));
        }

        /// Arrived rename targets, in the topologically-sorted order computed above.
        for (const auto & name : ordered_moves)
        {
            changed = true;
            if (eventMask() & DW_ITEM_MOVED_TO)
                owner.onItemMovedTo(DirectoryEvent(name, DW_ITEM_MOVED_TO));
        }

        /// Arrived identities that are genuinely new (including a name whose inode was replaced).
        for (const auto & [name, state] : current)
        {
            if (same_identity(snapshot, name, state.inode) || is_rename(state.inode))
                continue;
            changed = true;
            if (eventMask() & DW_ITEM_ADDED)
                owner.onItemAdded(DirectoryEvent(name, DW_ITEM_ADDED));
        }

        /// Same name and inode, but the contents changed (an append or truncation). A name that was
        /// unlinked and recreated is excluded here - it was already emitted as REMOVED + ADDED above.
        for (const auto & [name, state] : current)
        {
            if (deleted.contains(name))
                continue;
            auto it = snapshot.find(name);
            if (it == snapshot.end() || it->second.inode != state.inode)
                continue;
            if (it->second.mtime_ns != state.mtime_ns || it->second.size != state.size)
            {
                changed = true;
                if (eventMask() & DW_ITEM_MODIFIED)
                    owner.onItemModified(DirectoryEvent(name, DW_ITEM_MODIFIED));
            }
        }

        snapshot.swap(current);
        /// This pass committed successfully, so its drained deletes have been applied; start the next
        /// pass with a clean set. (On a retried pass we skip this via `continue`, keeping them.)
        deleted.clear();

        if (changed)
        {
            milliseconds_to_wait = (*settings)[FileLogSetting::poll_directory_watch_events_backoff_init].totalMilliseconds();
            owner.storage.wakeUp();
        }
        else if (milliseconds_to_wait < static_cast<uint64_t>((*settings)[FileLogSetting::poll_directory_watch_events_backoff_max].totalMilliseconds()))
        {
            milliseconds_to_wait *= (*settings)[FileLogSetting::poll_directory_watch_events_backoff_factor].value;
        }
    }
}
#endif

DirectoryWatcherBase::~DirectoryWatcherBase()
{
    stop();
#if defined(OS_LINUX)
    [[maybe_unused]] int err = ::close(inotify_fd);
    chassert(!err || errno == EINTR);
#endif
}

void DirectoryWatcherBase::start()
{
    if (watch_task)
        watch_task->activateAndSchedule();
}

void DirectoryWatcherBase::stop()
{
    stopped = true;
    (void)::write(event_pipe.fds_rw[1], "\0", 1);
    if (watch_task)
        watch_task->deactivate();
}

}
