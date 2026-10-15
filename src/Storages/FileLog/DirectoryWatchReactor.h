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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{
class DirectoryWatcherBase;

/// A single thread that multiplexes all `DirectoryWatcherBase` objects, so that the number of threads does not
/// depend on the number of `FileLog` tables.
///
/// On Linux the reactor owns one inotify descriptor, which it waits on with `epoll`, so that the number of inotify
/// instances does not depend on the number of tables either (`fs.inotify.max_user_instances` is only 128 by default).
/// The directory of each watcher is a watch of that descriptor: `add` adds it and `remove` removes it. The kernel
/// gives the watchers of the same directory the same watch, and the reactor hands its events to each of them. The
/// callbacks of a watcher (`DirectoryWatcherBase::onEvent`, `DirectoryWatcherBase::onReadEnd` and
/// `DirectoryWatcherBase::onPairingTimeout`) run on the reactor thread, one at a time, and must not block.
///
/// On macOS the reactor owns one `kqueue`. It registers the directory descriptor of a watcher in it in `add`, and the
/// watchers register the descriptors of their files (`watchVnode`), with the registration of the watcher as the user
/// data of the event. The reactor hands the
/// events to the owning watcher (`DirectoryWatcherBase::onVnodeEvent`), and the same single thread also scans the
/// directories of the watchers when their scan is due (`DirectoryWatcherBase::onTimer`).
class DirectoryWatchReactor
{
public:
#if defined(OS_LINUX)
    /// `pairing_timeout_` is one timeout for all watchers: it restarts whenever a watcher starts or keeps waiting for
    /// the second half of a rename, and when it expires every watcher that still waits is committed as it is.
    /// So each waiting watcher gets at least the whole timeout, but continuous renames in other directories can
    /// postpone the commit of a watcher whose second half never comes.
    explicit DirectoryWatchReactor(std::chrono::milliseconds pairing_timeout_);
#elif defined(OS_DARWIN)
    DirectoryWatchReactor();

    /// Called by a watcher from its callbacks, that is on the reactor thread only, once it has been added.
    /// Registers `fd` for the vnode events `fflags` (`NOTE_*`) of `watcher`, which the events of `fd` are reported to.
    /// Closing `fd` removes the registration.
    void watchVnode(DirectoryWatcherBase & watcher, int fd, uint32_t fflags);

    /// Called by a watcher from `onTimer`, that is on the reactor thread only.
    /// Hands the events that are ready in the kqueue right now, of this and of the other watchers, to their watchers
    /// without waiting. A watcher uses it to learn whether its directory changed while it was scanning it.
    void drainReadyEvents();
#endif

    /// All watchers must have been removed.
    ~DirectoryWatchReactor();

    /// Neither copyable nor movable: the reactor thread and the registrations refer to this very object, and it
    /// owns the `epoll` or `kqueue` descriptor and the control pipe.
    DirectoryWatchReactor(const DirectoryWatchReactor &) = delete;
    DirectoryWatchReactor & operator=(const DirectoryWatchReactor &) = delete;

    /// Starts watching the directory of `watcher`: on Linux it adds a watch of the directory to the inotify
    /// descriptor of the reactor, on macOS it registers the descriptor of the directory that the watcher has
    /// opened, and the reactor thread makes the first scan of the directory right after, once `add` has returned.
    /// Waits until the reactor thread has done it, and throws what it failed with.
    void add(DirectoryWatcherBase & watcher);

    /// Stops watching `watcher` and closes its descriptors. Waits until the reactor thread has done it, so
    /// that no callback of `watcher` is running or will run once this returns, and the watcher can be destroyed.
    /// Does nothing if the reactor has dropped the watcher after a failure of its callback.
    /// Must not be called from a callback.
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

    LoggerPtr log;

    /// The write end wakes the reactor thread up for a command or for shutdown.
    PipeFDs control_pipe;

    std::mutex mutex;
    std::deque<Command> commands;
    /// Set under `mutex`, so that no command is queued once the reactor thread has stopped taking them.
    bool stopped = false;

    /// Used only by the reactor thread.
    std::unordered_map<DirectoryWatcherBase *, std::unique_ptr<WatcherRegistration>> registrations;

    std::unique_ptr<ThreadFromGlobalPool> thread;

    /// What differs per platform: the loop, the registration of a watcher and what the loop waits on.
#if defined(OS_LINUX)
    struct WatcherRegistration
    {
        explicit WatcherRegistration(DirectoryWatcherBase & watcher_) : watcher(&watcher_) {}

        WatcherRegistration(const WatcherRegistration &) = delete;
        WatcherRegistration & operator=(const WatcherRegistration &) = delete;

        DirectoryWatcherBase * watcher;
        /// The watch of the directory in `inotify_fd`, shared by all watchers of the directory. -1 once the kernel has
        /// dropped it.
        int wd = -1;
        /// Its callback has failed in the current read, and it is removed at the end of it.
        bool failed = false;
        /// It has got events in the current read, and `onReadEnd` is to be called.
        bool touched = false;
    };

    void runLoop();
    void attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration);
    void detachWatcher(WatcherRegistration & registration);
    /// Reads the events of `inotify_fd` and hands them to the watchers.
    void dispatchEvents();
    /// Hands one event to the watchers of its watch, and adds those to `touched`.
    void dispatchEvent(const inotify_event & event, std::vector<WatcherRegistration *> & touched);
    void failWatcher(WatcherRegistration & registration);
    void removeFailedWatchers();
    void flushPairingTimeout();

    const std::chrono::milliseconds pairing_timeout;
    Epoll epoll;
    int inotify_fd = -1;

    /// The registrations of the watchers of each watch of `inotify_fd`.
    std::unordered_map<int, std::vector<WatcherRegistration *>> registrations_by_wd;
    std::vector<DirectoryWatcherBase *> failed_watchers;

    std::unordered_set<WatcherRegistration *> waiting_for_pairing;
    /// The events are read here and decoded at once, so one buffer serves all the watchers.
    std::string buffer;
    std::chrono::steady_clock::time_point pairing_deadline;
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
    void removeFailedWatchers();

    int kqueue_fd = -1;

    std::vector<DirectoryWatcherBase *> failed_watchers;
    /// `drainReadyEvents` has taken the wake-up of the control pipe, which is not reported again.
    bool commands_pending = false;
#endif
};

}

#endif
