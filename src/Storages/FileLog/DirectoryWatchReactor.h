#pragma once

#include <base/defines.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/Logger_fwd.h>
#include <Common/PipeFDs.h>
#include <Common/ThreadPool_fwd.h>

#if defined(OS_LINUX)
#include <Common/Epoll.h>

struct inotify_event;
#elif defined(OS_DARWIN)
#include <sys/event.h>
#endif

#include <array>
#include <chrono>
#include <deque>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{
class DirectoryWatcherBase;

/// One thread for all `DirectoryWatcherBase` objects, so threads and inotify instances don't grow with `FileLog` tables.
/// Linux: a fixed pool of inotify descriptors under `epoll`, a watch per directory, each watcher on a random descriptor
/// of the pool. Each descriptor has its own queue of events, so when a directory that changes all the time overflows
/// one, only the watchers of that descriptor lose events. macOS: one `kqueue` for directory and file descriptors, plus
/// timed scans. Watcher callbacks run on this thread, one at a time, and must not block.
class DirectoryWatchReactor
{
public:
    DirectoryWatchReactor();

#if defined(OS_DARWIN)
    /// Called by a watcher from its callbacks, that is on the reactor thread only, once it has been added.
    /// Registers `fd` for the vnode events `fflags` (`NOTE_*`) of `watcher`, which the events of `fd` are reported to.
    /// Closing `fd` removes the registration.
    void watchVnode(DirectoryWatcherBase & watcher, int fd, uint32_t fflags);

    /// Called from `onTimer` on the reactor thread. Dispatches the events ready in the kqueue now, without waiting, so
    /// a watcher learns whether its directory changed during a scan. Bounded so commands are not delayed: returns
    /// false if it stopped before taking all ready events, and then the caller cannot rely on what it got.
    [[nodiscard]] bool drainReadyEvents();
#endif

    /// All watchers must have been removed.
    ~DirectoryWatchReactor();

    /// Neither copyable nor movable: the reactor thread and the registrations refer to this very object, and it
    /// owns the `epoll` or `kqueue` descriptor and the control pipe.
    DirectoryWatchReactor(const DirectoryWatchReactor &) = delete;
    DirectoryWatchReactor & operator=(const DirectoryWatchReactor &) = delete;

    /// Starts watching the directory of `watcher`: an inotify watch on Linux, the watcher's directory descriptor on
    /// macOS (the first scan follows right after `add` returns). Waits until the reactor thread has done it, and
    /// throws what it failed with.
    void add(DirectoryWatcherBase & watcher);

    /// Stops watching `watcher` and closes its descriptors; once it returns no callback of `watcher` runs, so it can be
    /// destroyed. Does nothing if the reactor dropped the watcher after a failed callback. Must not be called from a
    /// callback.
    void remove(DirectoryWatcherBase & watcher);

private:
    static constexpr size_t max_events_per_wait = 64;

    struct Command
    {
        DirectoryWatcherBase * watcher;
        bool is_add;
        std::promise<void> done;
    };

    /// The registration of a watcher, the user data of its descriptors. Defined below, as it differs per platform.
    struct WatcherRegistration;

    void submit(Command command);
    void wakeUp();
    /// Makes the reactor thread leave `runLoop` and joins it.
    void stop();

    void run();
    void processCommands();
    void addWatcher(DirectoryWatcherBase & watcher);
    void removeWatcher(DirectoryWatcherBase & watcher);
    /// Marks the watcher as failed, so it gets no more events and is removed by `removeFailedWatchers`.
    void failWatcher(WatcherRegistration & registration);

    LoggerPtr log;

    /// The write end wakes the reactor thread up for a command or for shutdown.
    PipeFDs control_pipe;

    std::mutex mutex;
    std::deque<Command> commands;
    /// Set under `mutex`, so that no command is queued once the reactor thread has stopped taking them.
    bool stopped = false;

    /// Used only by the reactor thread.
    std::unordered_map<DirectoryWatcherBase *, std::unique_ptr<WatcherRegistration>> registrations;
    std::vector<DirectoryWatcherBase *> failed_watchers;

    std::unique_ptr<ThreadFromGlobalPool> thread;

    /// What differs per platform: the loop, the registration of a watcher and what the loop waits on.
#if defined(OS_LINUX)
    struct WatcherRegistration
    {
        explicit WatcherRegistration(DirectoryWatcherBase & watcher_) : watcher(&watcher_) {}

        WatcherRegistration(const WatcherRegistration &) = delete;
        WatcherRegistration & operator=(const WatcherRegistration &) = delete;

        DirectoryWatcherBase * watcher;
        /// The descriptor of `inotify_fds` the watcher is assigned to.
        int inotify_fd = -1;
        /// The watch of the directory in `inotify_fd`, shared by all watchers of the directory on that descriptor. A
        /// watcher gets only the events queued after it was added (see `attachWatcher`). -1 once the kernel has dropped it.
        int wd = -1;
        /// Its callback has failed in the current read, and it is removed at the end of it.
        bool failed = false;
    };

    void runLoop();
    /// Closes the descriptors of `inotify_fds` that are open.
    void closeInotifyDescriptors();
    void attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration);
    void detachWatcher(WatcherRegistration & registration);
    /// Reads the events of `inotify_fd` and hands them to the watchers.
    void dispatchEvents(int inotify_fd);
    /// Reads exactly the events that are in the queue of `inotify_fd` right now and hands them to the watchers, so
    /// that a watcher added next does not get them.
    void dispatchQueuedEvents(int inotify_fd);
    /// Commits the events of `touched_watchers` after a read, and removes the watchers that failed in it.
    void commitTouchedWatchers();
    /// Hands one event read from `inotify_fd` to the watchers of its watch, and adds those to `touched_watchers`.
    void dispatchEvent(int inotify_fd, const inotify_event & event);
    void removeFailedWatchers();
    /// Commits the watchers whose deadline is not after `now`.
    void flushExpiredPairings(std::chrono::steady_clock::time_point now);

    Epoll epoll;
    /// Each inotify descriptor can have at most 16384 events in the queue by default, so we use a pool of them to avoid overflow.
    static constexpr size_t inotify_instances = 8;
    std::array<int, inotify_instances> inotify_fds{};

    /// The registrations of the watchers of each watch, by the descriptor of `inotify_fds` and the watch: a watch
    /// descriptor is unique only within its inotify instance.
    std::map<std::pair<int, int>, std::vector<WatcherRegistration *>> registrations_by_wd;

    /// Watchers waiting for the second half of a rename split between two reads; few, so unsorted. Each sets its own
    /// deadline while decoding, and the loop scans all of them for the earliest and the expired ones. On expiry the
    /// watcher is committed as it is.
    std::unordered_set<WatcherRegistration *> waiting_for_pairing;
    /// The events are read here and decoded at once, so one buffer serves all the watchers.
    std::string buffer;
    /// The watchers that got events in the current read, to commit their events once per read. It is empty between
    /// the reads, and kept to reuse its memory.
    std::unordered_set<WatcherRegistration *> touched_watchers;
#elif defined(OS_DARWIN)
    struct WatcherRegistration
    {
        explicit WatcherRegistration(DirectoryWatcherBase & watcher_) : watcher(&watcher_) {}

        WatcherRegistration(const WatcherRegistration &) = delete;
        WatcherRegistration & operator=(const WatcherRegistration &) = delete;

        DirectoryWatcherBase * watcher;
        /// Its callback has failed in the current batch of events, and it is removed at the end of it.
        bool failed = false;
    };

    void runLoop();
    void attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration);
    void detachWatcher(DirectoryWatcherBase & watcher);
    /// Registers `fd` in the kqueue for the vnode events `fflags`, with `registration` as the user data.
    void registerVnode(WatcherRegistration & registration, int fd, uint32_t fflags);
    void dispatch(WatcherRegistration & registration, int fd, uint32_t fflags);
    size_t waitForEvents(std::array<struct kevent, max_events_per_wait> & events, int timeout_ms, bool & has_commands);
    void watcherScan(WatcherRegistration & registration);
    /// Commits the events of `touched_watchers` after the due scans or a batch of events, and removes the watchers
    /// that failed in them.
    void commitTouchedWatchers();
    void removeFailedWatchers();

    int kqueue_fd = -1;

    /// The watchers that reported events in the current scans or batch of events, to commit their events once. It is
    /// empty between them, and kept to reuse its memory. A scan drains the events of all the watchers, so it can add
    /// other watchers than the scanned one.
    std::unordered_set<WatcherRegistration *> touched_watchers;

    /// `drainReadyEvents` has taken the wake-up of the control pipe, which is not reported again.
    bool commands_pending = false;
#endif
};

}

#endif
