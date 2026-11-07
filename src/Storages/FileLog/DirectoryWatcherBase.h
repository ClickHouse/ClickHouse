#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstdint>
#include <memory>
#include <string>
#if defined(OS_LINUX)
#include <chrono>
#include <deque>
#include <unordered_set>

struct inotify_event;
#elif defined(OS_DARWIN)
#include <Storages/FileLog/DarwinDirectoryWatchState.h>

#include <chrono>
#include <map>
#include <optional>
#include <vector>
#endif

namespace DB
{
class FileLogDirectoryWatcher;
class DirectoryWatchReactor;

class DirectoryWatcherBase : WithContext
{
    /// Reports changes of one directory (not its subdirectories), based on Poco's `DirectoryWatcher`. Events come from the
    /// `DirectoryWatchReactor` thread: on Linux via a watch of its inotify descriptor, on macOS by diffing snapshots
    /// of the directory, woken up by `kqueue` events and a timer.
public:
    enum DirectoryEventType
    {
        DW_ITEM_ADDED = 1,
        /// A new item has been created and added to the directory.

        DW_ITEM_REMOVED = 2,
        /// An item has been removed from the directory.

        DW_ITEM_MODIFIED = 4,
        /// An item has been modified.

        DW_ITEM_MOVED_FROM = 8,
        /// An item has been renamed or moved. This event delivers the old name.

        DW_ITEM_MOVED_TO = 16
        /// An item has been renamed within the directory. This event delivers the new name.
        /// An item moved in from elsewhere is reported as DW_ITEM_ADDED.
    };

    enum DirectoryEventMask
    {
        /// Enables all event types.
        DW_FILTER_ENABLE_ALL = 31,

        /// Disables all event types.
        DW_FILTER_DISABLE_ALL = 0
    };

    struct DirectoryEvent
    {
        DirectoryEvent(const std::string & f, DirectoryEventType ev, uint64_t cookie_ = 0) : path(f), event(ev), cookie(cookie_) { }

        /// The directory or file that has been changed.
        const std::string path;
        /// The kind of event.
        DirectoryEventType event;
        /// The same value on the DW_ITEM_MOVED_FROM and DW_ITEM_MOVED_TO of one rename, 0 otherwise.
        uint64_t cookie;
    };


    DirectoryWatcherBase() = delete;
    DirectoryWatcherBase(const DirectoryWatcherBase &) = delete;
    DirectoryWatcherBase & operator=(const DirectoryWatcherBase &) = delete;

    /// Creates a DirectoryWatcher for the directory given in path.
    /// To enable only specific events, an eventMask can be specified by
    /// OR-ing the desired event IDs (e.g., DW_ITEM_ADDED | DW_ITEM_MODIFIED).
    explicit DirectoryWatcherBase(
        FileLogDirectoryWatcher & owner_, const std::string & path_, ContextPtr context_, int event_mask_ = DW_FILTER_ENABLE_ALL);

    ~DirectoryWatcherBase();

    /// Returns the value of the eventMask passed to the constructor.
    int eventMask() const { return event_mask; }

    /// Returns the directory being watched.
    const std::string & directory() const;

private:
    friend class DirectoryWatchReactor;

    FileLogDirectoryWatcher & owner;

    const std::string path;
    int event_mask;

    /// Kept alive by the watcher, so that `~DirectoryWatcherBase` can always unregister from it.
    std::shared_ptr<DirectoryWatchReactor> reactor;
    /// Whether the reactor watches the directory. Not if it failed to add the watcher, then the watcher stays inert.
    bool registered = false;

    /// Whether the watcher has reported an event to `owner` that has not been committed yet.
    bool has_uncommitted_events = false;

    /// Called by the `DirectoryWatchReactor` thread only, once after a batch of events or scans that reported events
    /// to `owner`: hands them to `owner` at once and wakes up the reader thread.
    void commitEvents();

#if defined(OS_LINUX)
    /// Cookies of the `IN_MOVED_FROM` events that are waiting for their `IN_MOVED_TO`, the oldest evicted first.
    std::deque<uint32_t> unpaired_move_cookies;
    /// Cookies of the `IN_MOVED_FROM` events of the current batch that have not got their `IN_MOVED_TO` yet.
    std::unordered_set<uint32_t> unpaired_moves;
    /// How long the watcher waits for the `IN_MOVED_TO` of a rename that one read of the inotify descriptor did not bring.
    static constexpr std::chrono::milliseconds pairing_timeout{10};
    /// Set to `pairing_timeout` from now when the watcher starts to wait for the second half of a rename, and not
    /// extended by later renames. The `DirectoryWatchReactor` commits the watcher at this time unless its renames
    /// complete before.
    std::chrono::steady_clock::time_point pairing_deadline;

    /// The inotify mask for the events the watcher is interested in.
    uint32_t watchMask() const;

    /// Reactor thread only. `onEvent` decodes one event; `hasUnpairedMoves` is checked after each read of inotify. If
    /// every rename is complete, the reactor calls `commitEvents` to hand the events to `owner`, otherwise it calls
    /// `onPairingTimeout` unless the rest of the events come in time.
    void onEvent(const inotify_event & event);
    bool hasUnpairedMoves() const;
    void onPairingTimeout();
    /// The kernel has dropped the watch of the directory (`IN_IGNORED`), because it was deleted or unmounted.
    void onWatchRemoved();
#elif defined(OS_DARWIN)
    /// The callbacks below are called by the `DirectoryWatchReactor` thread only.
    std::unique_ptr<DarwinDirectoryWatchState> watch_state;

    /// The descriptor of the directory, opened by the constructor, and the vnode events (`NOTE_*`) to watch it for.
    int directoryFd() const;
    uint32_t directoryWatchFlags() const;
    /// Closes all the descriptors of the watcher.
    void stopWatching();
    /// An event of one of the descriptors registered with `DirectoryWatchReactor::watchVnode`. Reports a change of the
    /// contents of a file at once, and schedules a scan for a change of the listing. The reactor commits the reported
    /// events after the batch, as it does after `onTimer`.
    void onVnodeEvent(int fd, uint32_t fflags);
    /// Scans the directory and emits the events of what has changed since the last scan, once `nextScan` is due.
    void onTimer();
    /// When `onTimer` is due, `nullopt` if the watcher does not watch.
    std::optional<std::chrono::steady_clock::time_point> nextScan() const;

    void scan(std::map<std::string, UInt64> & current_files_inodes);
    /// The one of the excluded names `aliases` of the followed file `inode` that `scan` lists it under.
    std::string chooseFollowedAlias(UInt64 inode, std::vector<std::string> & aliases) const;
    void syncFileWatches(std::map<std::string, UInt64> & files);
    /// Returns whether the directory changed during the scan, then the caller discards it.
    bool scanFilesAndUpdateWatches(std::map<std::string, UInt64> & current_files_inodes);
    /// Reports the `deferred_modifications` under the names of the committed listing `current_files_inodes`, and
    /// returns whether it reported any. The inodes the listing does not have anymore are dropped.
    bool emitDeferredModifications(const std::map<std::string, UInt64> & current_files_inodes);
#endif
};

}
