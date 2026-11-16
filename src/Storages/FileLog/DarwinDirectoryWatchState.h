#pragma once

#include <base/defines.h>

#if defined(OS_DARWIN)

#include <base/types.h>

#include <chrono>
#include <map>
#include <set>
#include <string>
#include <unordered_map>

namespace DB
{

/// The state of the macOS directory watcher.
/// It is used only by the `DirectoryWatchReactor` thread (and the constructor, before the watcher is added).
struct DarwinDirectoryWatchState
{
    /// A descriptor that is registered in the kqueue of the reactor, pinned to the inode it was opened for.
    struct WatchedFd
    {
        int fd;
        UInt64 inode;
    };

    /// The identity of a file and a fingerprint of its contents, as of a scan. The fingerprint lets the scan report
    /// an append or truncation that kqueue did not, e.g. on a network file system.
    struct FileState
    {
        UInt64 inode;
        Int64 mtime_ns;
        Int64 size;
    };

    /// The name of each listed file to its state.
    using Listing = std::map<std::string, FileState>;

    explicit DarwinDirectoryWatchState(UInt64 milliseconds_to_wait_) : milliseconds_to_wait(milliseconds_to_wait_) {}
    ~DarwinDirectoryWatchState();

    DarwinDirectoryWatchState(const DarwinDirectoryWatchState &) = delete;
    DarwinDirectoryWatchState & operator=(const DarwinDirectoryWatchState &) = delete;

    void watchFile(const std::string & name, int fd, UInt64 inode);
    std::map<std::string, WatchedFd>::iterator unwatchFile(std::map<std::string, WatchedFd>::iterator it);

    /// Commit the listing of a successful scan as the snapshot, and follow its files under their current names.
    /// Keep `snapshot` and `followed_files` in sync, so call it only after the scan succeeded: a failed or
    /// discarded listing may lack followed files, which then would no longer be listed under an excluded name.
    void setSnapshot(Listing && new_snapshot);

    /// The inode of each followed file to the name it was watched (or read) under, see `DirectoryWatcherBase::scan`.
    std::unordered_map<UInt64, std::string> followed_files;

    /// The name of each file of the directory to its state, as of the last scan.
    Listing snapshot;
    std::map<std::string, WatchedFd> watched_fds;
    /// The name of each descriptor of `watched_fds`, to find the file that an event of the descriptor is for.
    std::unordered_map<int, std::string> watched_names;
    int dir_fd = -1;

    /// File names whose watched inode lost its last link (`NOTE_DELETE`), accumulated across the scans, so a retried
    /// scan keeps them: with `EV_CLEAR` a dropped `NOTE_DELETE` is never reported again, and a same-inode recreate
    /// would slip through as a modification.
    std::set<std::string> deleted_files;

    /// An entry of the directory was added, removed or renamed since the last reset. `scanFilesAndUpdateWatches`
    /// uses it to learn whether the directory changed while it was being scanned, then the listing can not be trusted.
    bool directory_changed = false;

    /// An event said that the listing, or the name of a watched file, may have changed, and no scan has committed it
    /// yet. Until then `watched_names` may map a descriptor to a name that now belongs to another file.
    bool structural_change_pending = false;

    /// Inodes of watched files modified while `structural_change_pending` was set. The scan that commits the change
    /// reports them after its events, under the names of its listing, so a modification is never reported under a
    /// stale name. Keyed by inode, as the name is what may be stale.
    std::set<UInt64> deferred_modifications;

    /// Inodes of watched files whose modification `onVnodeEvent` reported at once since the last committed scan. The
    /// scan does not report their changed fingerprint again, so a file being written does not reset the backoff.
    std::set<UInt64> reported_modifications;

    /// Whether the first snapshot, the baseline of the files that already existed, has been taken. Until then
    /// the scans only take the snapshot and emit no events.
    bool has_snapshot = false;

    UInt64 milliseconds_to_wait;

    /// When the watcher is to run `onTimer` next. Changed by `onVnodeEvent` (to now) and by `onTimer`.
    std::chrono::steady_clock::time_point next_scan = std::chrono::steady_clock::now();
};

}

#endif
