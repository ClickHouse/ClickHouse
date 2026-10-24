#include <Storages/FileLog/DirectoryWatchReactor.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Storages/FileLog/DirectoryWatcherBase.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <optional>
#include <unistd.h>
#if defined(OS_LINUX)
#include <random>
#include <sys/inotify.h>
#include <sys/ioctl.h>
#elif defined(OS_DARWIN)
#include <fcntl.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int IO_SETUP_ERROR;
}

void DirectoryWatchReactor::stop()
{
    {
        std::lock_guard lock(mutex);
        stopped = true;
    }
    wakeUp();

    /// Mandatory: ~ThreadFromGlobalPoolImpl aborts on a still-joinable thread.
    if (thread->joinable())
        thread->join();

    chassert(registrations.empty());
    chassert(commands.empty());
}

void DirectoryWatchReactor::add(DirectoryWatcherBase & watcher)
{
    submit(Command{&watcher, /* is_add = */ true, {}});
}

void DirectoryWatchReactor::remove(DirectoryWatcherBase & watcher)
{
    submit(Command{&watcher, /* is_add = */ false, {}});
}

void DirectoryWatchReactor::submit(Command command)
{
    std::future<void> result = command.done.get_future();
    {
        std::lock_guard lock(mutex);
        if (stopped)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The directory watch reactor is stopped");

        commands.push_back(std::move(command));
    }
    wakeUp();

    /// Rethrows the failure of the command, if any.
    result.get();
}

void DirectoryWatchReactor::wakeUp()
{
    /// EAGAIN means that the pipe is full of wake-ups the reactor thread has not read yet, which is as good as this one.
    [[maybe_unused]] auto res = ::write(control_pipe.fds_rw[1], "\0", 1);
}

void DirectoryWatchReactor::run()
{
    setThreadName(ThreadName::FILE_LOG_WATCH);

    try
    {
        runLoop();
    }
    catch (...)
    {
        /// The loop fails only if `epoll` itself does, and there is nothing to continue with then. Do not leave the
        /// callers of `add` and `remove` waiting for a thread that is gone.
        tryLogCurrentException(log, "The directory watch reactor stopped");

        std::deque<Command> pending;
        {
            std::lock_guard lock(mutex);
            stopped = true;
            pending.swap(commands);
        }
        for (auto & command : pending)
            command.done.set_exception(std::current_exception());
    }
}

void DirectoryWatchReactor::processCommands()
{
    /// Drain the wake-ups first, so that one that comes in while the commands are processed is not lost.
    char wakeups[64];
    while (true)
    {
        const ssize_t res = ::read(control_pipe.fds_rw[0], wakeups, sizeof(wakeups));
        if (res > 0 || (res < 0 && errno == EINTR))
            continue;
        if (res < 0 && errno != EAGAIN)
            throw ErrnoException(
                ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from the control pipe of the directory watch reactor");
        /// The pipe is empty: `read` failed with `EAGAIN` because the descriptor is non-blocking. It cannot return 0
        /// (end of file), as this object holds the write end open for as long as the reactor thread runs.
        break;
    }

    std::deque<Command> pending;
    {
        std::lock_guard lock(mutex);
        pending.swap(commands);
    }

    for (auto & command : pending)
    {
        try
        {
            if (command.is_add)
                addWatcher(*command.watcher);
            else
                removeWatcher(*command.watcher);
            command.done.set_value();
        }
        catch (...)
        {
            command.done.set_exception(std::current_exception());
        }
    }
}

void DirectoryWatchReactor::addWatcher(DirectoryWatcherBase & watcher)
{
    if (registrations.contains(&watcher))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The directory watcher of {} is already added to the reactor", watcher.path);

    attachWatcher(watcher, std::make_unique<WatcherRegistration>(watcher));
}

void DirectoryWatchReactor::removeWatcher(DirectoryWatcherBase & watcher)
{
    /// Not registered if it was already dropped after a failure of its callback.
    auto it = registrations.find(&watcher);
    if (it == registrations.end())
        return;

    /// Taken out first, so that the registration is gone and its descriptors closed even if the rest fails.
    std::unique_ptr<WatcherRegistration> registration = std::move(it->second);
    registrations.erase(it);
#if defined(OS_LINUX)
    detachWatcher(*registration);
#elif defined(OS_DARWIN)
    detachWatcher(watcher);
#endif
}

void DirectoryWatchReactor::failWatcher(WatcherRegistration & registration)
{
    registration.failed = true;
    failed_watchers.push_back(registration.watcher);
}

#if defined(OS_LINUX)

static constexpr size_t buffer_size = 4096;

/// At most this many reads of the inotify descriptor per wake-up, so that a directory that changes all the time does
/// not delay the commands. The descriptor is level-triggered, so the rest of the events wakes the loop up at once.
static constexpr size_t max_reads_per_wakeup = 64;

DirectoryWatchReactor::DirectoryWatchReactor()
    : log(getLogger("DirectoryWatchReactor"))
    , epoll(EpollNesting::Leaf)
    , buffer(buffer_size, '\0')
{
    control_pipe.setNonBlockingReadWrite();
    inotify_fds.fill(-1);

    try
    {
        /// The epoll data comes back with every event of the descriptor, so that the loop knows what the event is for:
        /// the control pipe is registered with `this`, it is a wake-up for a command, each inotify descriptor is
        /// registered with its own address in `inotify_fds`.
        epoll.add(control_pipe.fds_rw[0], this);
        for (int & inotify_fd : inotify_fds)
        {
            inotify_fd = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
            if (inotify_fd == -1)
                throw ErrnoException(
                    ErrorCodes::IO_SETUP_ERROR,
                    "Cannot initialize inotify for the directory watch reactor, see `fs.inotify.max_user_instances`");
            epoll.add(inotify_fd, &inotify_fd);
        }

        thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
    }
    catch (...)
    {
        closeInotifyDescriptors();
        throw;
    }
}

DirectoryWatchReactor::~DirectoryWatchReactor()
{
    stop();

    chassert(registrations_by_wd.empty());
    closeInotifyDescriptors();
}

void DirectoryWatchReactor::closeInotifyDescriptors()
{
    for (int inotify_fd : inotify_fds)
    {
        if (inotify_fd == -1)
            continue;
        int err = ::close(inotify_fd);
        chassert(!err || errno == EINTR);
    }
}

void DirectoryWatchReactor::runLoop()
{
    std::array<epoll_event, max_events_per_wait> events{};

    while (true)
    {
        /// The wait ends at the earliest deadline of the waiting watchers. Once it has passed the wait does not block,
        /// and the events already in the queue are dispatched before the flush below, so that a rename split between
        /// two reads is completed instead of committing only its first half.
        int timeout_ms = -1;
        if (!waiting_for_pairing.empty())
        {
            auto earliest_deadline = std::chrono::steady_clock::time_point::max();
            for (const WatcherRegistration * registration : waiting_for_pairing)
                earliest_deadline = std::min(earliest_deadline, registration->watcher->pairing_deadline);

            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(earliest_deadline - std::chrono::steady_clock::now());
            timeout_ms = static_cast<int>(std::max<std::chrono::milliseconds::rep>(remaining.count(), 0));
        }

        const size_t ready = epoll.getManyReady(static_cast<int>(events.size()), events.data(), timeout_ms);

        /// The commands go after the watchers: a command can remove a watcher that has an event in this very batch.
        bool has_commands = false;
        for (size_t i = 0; i < ready; ++i)
        {
            if (events[i].data.ptr == this)
                has_commands = true;
            else
                dispatchEvents(*static_cast<const int *>(events[i].data.ptr));
        }

        if (has_commands)
            processCommands();

        if (!waiting_for_pairing.empty())
            flushExpiredPairings(std::chrono::steady_clock::now());

        /// Once `stopped` is set no command is submitted, so the queue stays empty and nothing is lost by leaving.
        std::lock_guard lock(mutex);
        if (stopped && commands.empty())
            return;
    }
}

void DirectoryWatchReactor::attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration)
{
    //select one inotify fd at random from the pool
    const int inotify_fd = inotify_fds[std::uniform_int_distribution<size_t>(0, inotify_instances - 1)(thread_local_rng)];
    registration->inotify_fd = inotify_fd;

    /// The kernel shares one watch per directory in an inotify instance, so the queue can hold events from before this
    /// watcher's baseline. Hand them to the other watchers first, before adding the watch; otherwise events between the
    /// two would reach the other watchers only, and this watcher would lose events that happened after its baseline.
    dispatchQueuedEvents(inotify_fd);

    /// Moved into `registrations` before its address is stored in `registrations_by_wd`. If `emplace` throws, the
    /// registration is destroyed, and no pointer to it is left behind for `dispatchEvent` to read.
    const auto it = registrations.emplace(&watcher, std::move(registration)).first;
    WatcherRegistration & new_registration = *it->second;
    try
    {
        /// `IN_MASK_ADD` keeps the events of the other watchers of the watch. Each watcher filters the events it gets by
        /// its own mask.
        const int wd = inotify_add_watch(inotify_fd, watcher.path.c_str(), watcher.watchMask() | IN_MASK_ADD);
        if (wd == -1)
        {
            if (errno == ENOSPC)
                throw ErrnoException(
                    ErrorCodes::IO_SETUP_ERROR,
                    "Watch directory {} failed: the limit of inotify watches is reached, see `fs.inotify.max_user_watches`",
                    watcher.path);
            ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, watcher.path, "Watch directory {} failed", watcher.path);
        }

        const std::pair<int, int> key{inotify_fd, wd};
        const bool is_new_watch = !registrations_by_wd.contains(key);
        try
        {
            registrations_by_wd[key].push_back(&new_registration);
        }
        catch (...)
        {
            /// A watch shared with other watchers is kept: its mask may have been widened by `IN_MASK_ADD`, which is
            /// harmless, as each watcher filters the events by its own mask.
            if (is_new_watch)
            {
                registrations_by_wd.erase(key);
                inotify_rm_watch(inotify_fd, wd);
            }
            throw;
        }
        new_registration.wd = wd;
    }
    catch (...)
    {
        registrations.erase(it);
        throw;
    }
}

void DirectoryWatchReactor::detachWatcher(WatcherRegistration & registration)
{
    waiting_for_pairing.erase(&registration);

    /// The kernel has dropped the watch already.
    if (registration.wd == -1)
        return;

    auto it = registrations_by_wd.find({registration.inotify_fd, registration.wd});
    chassert(it != registrations_by_wd.end());
    std::erase(it->second, &registration);
    if (!it->second.empty())
        return;

    registrations_by_wd.erase(it);
    /// The events of the watch that are still in the queue, and its `IN_IGNORED`, find no watchers and are skipped.
    /// `EINVAL` means that the kernel has dropped the watch already, and its `IN_IGNORED` is still in the queue.
    if (inotify_rm_watch(registration.inotify_fd, registration.wd) == -1 && errno != EINVAL)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot stop watching directory {}", registration.watcher->path);
}

void DirectoryWatchReactor::dispatchEvents(int inotify_fd)
{
    chassert(touched_watchers.empty());

    for (size_t reads = 0; reads < max_reads_per_wakeup; ++reads)
    {
        const ssize_t bytes_read = ::read(inotify_fd, buffer.data(), buffer.size());
        if (bytes_read < 0)
        {
            if (errno == EINTR)
                continue;
            /// The queue is empty.
            if (errno == EAGAIN)
                return;
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read the events of the directory watchers");
        }

        /// A read returns whole events only.
        for (size_t offset = 0; offset < static_cast<size_t>(bytes_read);)
        {
            const auto & event = *reinterpret_cast<const inotify_event *>(buffer.data() + offset);
            dispatchEvent(inotify_fd, event);
            offset += sizeof(inotify_event) + event.len;
        }

        commitTouchedWatchers();
    }
}

void DirectoryWatchReactor::dispatchQueuedEvents(int inotify_fd)
{
    chassert(touched_watchers.empty());

    /// The size of the whole events in the queue right now. The reads below take exactly these, and leave the events
    /// that come after to the next reads, which a watcher added next gets too.
    int queued = 0;
    if (::ioctl(inotify_fd, FIONREAD, &queued) == -1)
        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot get the size of the events of the directory watchers");

    size_t remaining = static_cast<size_t>(queued);
    while (remaining > 0)
    {
        /// The queue is read in order and in whole events, so the end of `remaining` is always the end of an event.
        const ssize_t bytes_read = ::read(inotify_fd, buffer.data(), std::min(buffer.size(), remaining));
        if (bytes_read < 0)
        {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN)
                return;
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read the events of the directory watchers");
        }
        if (bytes_read == 0)
            return;

        for (size_t offset = 0; offset < static_cast<size_t>(bytes_read);)
        {
            const auto & event = *reinterpret_cast<const inotify_event *>(buffer.data() + offset);
            dispatchEvent(inotify_fd, event);
            offset += sizeof(inotify_event) + event.len;
        }

        commitTouchedWatchers();
        remaining -=std::min(remaining, static_cast<size_t>(bytes_read));
    }
}

void DirectoryWatchReactor::commitTouchedWatchers()
{
    for (WatcherRegistration * registration : touched_watchers)
    {
        if (registration->failed)
            continue;

        try
        {
            if (registration->watcher->hasUnpairedMoves())
            {
                /// The deadline is kept by the watcher, so a watcher that waits already needs nothing more.
                waiting_for_pairing.insert(registration);
            }
            else
            {
                registration->watcher->commitEvents();
                waiting_for_pairing.erase(registration);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot process the events of a directory watcher, it is not watched anymore");
            failWatcher(*registration);
        }
    }
    touched_watchers.clear();

    removeFailedWatchers();
}

void DirectoryWatchReactor::dispatchEvent(int inotify_fd, const inotify_event & event)
{
    if (event.mask & IN_Q_OVERFLOW)
    {
        LOG_WARNING(log, "The queue of inotify descriptor {} has overflowed, so some changes of the directories watched with it "
            "are lost. Consider increasing `fs.inotify.max_queued_events`", inotify_fd);
        return;
    }

    /// The events of a watch that has been removed, which were still in the queue.
    auto it = registrations_by_wd.find({inotify_fd, event.wd});
    if (it == registrations_by_wd.end())
        return;

    if (event.mask & IN_IGNORED)
    {
        /// The kernel has dropped the watch, because the directory was deleted or unmounted.
        std::vector<WatcherRegistration *> watchers = std::move(it->second);
        registrations_by_wd.erase(it);
        for (WatcherRegistration * registration : watchers)
        {
            registration->wd = -1;
            if (registration->failed)
                continue;

            try
            {
                registration->watcher->onWatchRemoved();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Cannot handle the removal of the watch of a directory watcher, it is not watched anymore");
                failWatcher(*registration);
            }
        }
        return;
    }

    for (WatcherRegistration * registration : it->second)
    {
        if (registration->failed)
            continue;

        try
        {
            registration->watcher->onEvent(event);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot process an event of a directory watcher, it is not watched anymore");
            failWatcher(*registration);
            continue;
        }

        touched_watchers.insert(registration);
    }
}

void DirectoryWatchReactor::removeFailedWatchers()
{
    /// Cleared even if a removal throws, so the next call does not see the watchers of this one.
    SCOPE_EXIT({ failed_watchers.clear(); });
    for (DirectoryWatcherBase * watcher : failed_watchers)
        removeWatcher(*watcher);
}

void DirectoryWatchReactor::flushExpiredPairings(std::chrono::steady_clock::time_point now)
{
    for (auto it = waiting_for_pairing.begin(); it != waiting_for_pairing.end();)
    {
        WatcherRegistration * registration = *it;
        if (registration->watcher->pairing_deadline > now)
        {
            ++it;
            continue;
        }

        /// Erased before the callback; the callbacks do not change `waiting_for_pairing`, so `it` stays valid.
        it = waiting_for_pairing.erase(it);
        try
        {
            registration->watcher->onPairingTimeout();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot handle the pairing timeout of a directory watcher, it is not watched anymore");
            failWatcher(*registration);
        }
    }

    removeFailedWatchers();
}

#elif defined(OS_DARWIN)

DirectoryWatchReactor::DirectoryWatchReactor()
    : log(getLogger("DirectoryWatchReactor"))
{
    control_pipe.setNonBlockingReadWrite();

    kqueue_fd = ::kqueue();
    if (kqueue_fd == -1)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot create kqueue for the directory watch reactor");

    try
    {
        /// The user data comes back with every event of the descriptor, so that the loop knows what the event is for.
        /// The control pipe is registered with `this`: an event with `this` as its user data is a wake-up for a command,
        /// any other event belongs to the watcher whose registration it carries.
        struct kevent change{};
        /// Edge-triggered, as `drainReadyEvents` must not get the pipe again and again; `commands_pending` remembers it.
        EV_SET(&change, control_pipe.fds_rw[0], EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, this);
        if (::kevent(kqueue_fd, &change, 1, nullptr, 0, nullptr) == -1)
            throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot register the control pipe of the directory watch reactor");

        thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
    }
    catch (...)
    {
        int err = ::close(kqueue_fd);
        chassert(!err || errno == EINTR);
        throw;
    }
}

DirectoryWatchReactor::~DirectoryWatchReactor()
{
    stop();

    int err = ::close(kqueue_fd);
    chassert(!err || errno == EINTR);
}

size_t DirectoryWatchReactor::waitForEvents(std::array<struct kevent, max_events_per_wait> & events, int timeout_ms, bool & has_commands)
{
    struct timespec timeout{};
    if (timeout_ms >= 0)
    {
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_nsec = (timeout_ms % 1000) * 1'000'000L;
    }
    const int res = ::kevent(kqueue_fd, nullptr, 0, events.data(), static_cast<int>(events.size()), timeout_ms >= 0 ? &timeout : nullptr);
    if (res < 0)
    {
        if (errno == EINTR)
            return 0;
        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot wait for the events of the directory watch reactor");
    }

    /// The commands go after the watchers: a command can remove a watcher that has an event in this very batch.
    size_t watcher_events = 0;
    for (int i = 0; i < res; ++i)
    {
        if (events[i].udata == this)
            has_commands = true;
        else
            events[watcher_events++] = events[i];
    }
    return watcher_events;
}


static constexpr size_t max_drain_batches = 16;

/// Dispatches the events that are ready in the kqueue right now to their watchers, without waiting.
/// Returns true if it took all of them, false if it stopped after `max_drain_batches` full batches.
bool DirectoryWatchReactor::drainReadyEvents()
{
    std::array<struct kevent, max_events_per_wait> events{};
    for (size_t batches = 0; batches < max_drain_batches; ++batches)
    {
        /// `waitForEvents` drops the wake-up of the control pipe from the batch, so the size of the batch is checked
        /// together with it.
        bool has_commands = false;
        const size_t ready = waitForEvents(events, 0, has_commands);
        commands_pending |= has_commands;
        for (size_t i = 0; i < ready; ++i)
            dispatch(*static_cast<WatcherRegistration *>(events[i].udata), static_cast<int>(events[i].ident), events[i].fflags);

        /// A batch that is not full has drained the queue. Don't wait for later events: with `EV_CLEAR` a file that is
        /// written all the time is reported again at once, and the drain would never end.
        if (ready + (has_commands ? 1 : 0) < max_events_per_wait)
            return true;
    }
    return false;
}

void DirectoryWatchReactor::runLoop()
{
    std::array<struct kevent, max_events_per_wait> events{};

    while (true)
    {
        /// Scan due watchers between event batches: a scan reopens descriptors, so a pending event could refer to a
        /// reused descriptor number. Collect them first, so a watcher that is due again at once can't starve the others.
        std::vector<WatcherRegistration *> due;
        const auto now = std::chrono::steady_clock::now();
        for (const auto & [watcher, registration] : registrations)
        {
            const auto next_scan = watcher->nextScan();
            if (next_scan && *next_scan <= now)
                due.push_back(registration.get());
        }
        for (WatcherRegistration * registration : due)
            watcherScan(*registration);
        commitTouchedWatchers();

        /// A wake-up of the control pipe taken by `drainReadyEvents` during a scan. The commands are processed here,
        /// out of any callback, and before the timeout is computed, so that the scan of an added watcher is not delayed.
        if (std::exchange(commands_pending, false))
            processCommands();

        /// Sleep until the earliest scan, or until an event or a command if no watcher has one.
        int timeout_ms = -1;
        std::optional<std::chrono::steady_clock::time_point> earliest_scan;
        for (const auto & [watcher, registration] : registrations)
        {
            const auto next_scan = watcher->nextScan();
            if (next_scan && (!earliest_scan || *next_scan < *earliest_scan))
                earliest_scan = next_scan;
        }
        if (earliest_scan)
        {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*earliest_scan - std::chrono::steady_clock::now());
            timeout_ms = static_cast<int>(std::max<std::chrono::milliseconds::rep>(remaining.count(), 0));
        }

        bool has_commands = false;
        const size_t ready = waitForEvents(events, timeout_ms, has_commands);
        for (size_t i = 0; i < ready; ++i)
            dispatch(*static_cast<WatcherRegistration *>(events[i].udata), static_cast<int>(events[i].ident), events[i].fflags);
        commitTouchedWatchers();

        if (has_commands)
            processCommands();

        /// Once `stopped` is set no command is submitted, so the queue stays empty and nothing is lost by leaving.
        std::lock_guard lock(mutex);
        if (stopped && commands.empty())
            return;
    }
}

void DirectoryWatchReactor::watcherScan(WatcherRegistration & registration)
{
    if (registration.failed)
        return;

    try
    {
        registration.watcher->onTimer();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot scan the directory of a directory watcher, it is not watched anymore");
        failWatcher(registration);
        return;
    }

    touched_watchers.insert(&registration);
}

void DirectoryWatchReactor::commitTouchedWatchers()
{
    for (WatcherRegistration * registration : touched_watchers)
    {
        if (registration->failed)
            continue;

        try
        {
            registration->watcher->commitEvents();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot commit the events of a directory watcher, it is not watched anymore");
            failWatcher(*registration);
        }
    }
    touched_watchers.clear();

    removeFailedWatchers();
}

/// The watchers that failed are removed once nothing refers to their registrations anymore.
void DirectoryWatchReactor::removeFailedWatchers()
{
    /// Cleared even if a removal throws, so the next call does not see the watchers of this one.
    SCOPE_EXIT({ failed_watchers.clear(); });
    for (DirectoryWatcherBase * watcher : failed_watchers)
        removeWatcher(*watcher);
}

void DirectoryWatchReactor::attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration)
{
    /// Moved into `registrations` before its address is passed to the kqueue as the user data of the events. If
    /// `emplace` throws, the registration is destroyed, and the kqueue has no pointer to it to return in an event.
    const auto it = registrations.emplace(&watcher, std::move(registration)).first;
    try
    {
        registerVnode(*it->second, watcher.directoryFd(), watcher.directoryWatchFlags());
    }
    catch (...)
    {
        /// `kevent` has failed, so the kqueue does not hold the address of the registration.
        registrations.erase(it);
        throw;
    }
}

void DirectoryWatchReactor::detachWatcher(DirectoryWatcherBase & watcher)
{
    /// Closes the descriptors of the watcher, which removes them from the kqueue.
    watcher.stopWatching();
}

void DirectoryWatchReactor::watchVnode(DirectoryWatcherBase & watcher, int fd, uint32_t fflags)
{
    auto it = registrations.find(&watcher);
    if (it == registrations.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The directory watcher of {} is not added to the reactor", watcher.path);
    registerVnode(*it->second, fd, fflags);
}

void DirectoryWatchReactor::registerVnode(WatcherRegistration & registration, int fd, uint32_t fflags)
{
    struct kevent change{};
    EV_SET(&change, fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, fflags, 0, &registration);
    if (::kevent(kqueue_fd, &change, 1, nullptr, 0, nullptr) == -1)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot register a descriptor of a directory watcher");
}

void DirectoryWatchReactor::dispatch(WatcherRegistration & registration, int fd, uint32_t fflags)
{
    /// An earlier event of this batch has failed it, it is removed once the batch is done.
    if (registration.failed)
        return;

    try
    {
        registration.watcher->onVnodeEvent(fd, fflags);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot process the events of a directory watcher, it is not watched anymore");
        /// Not removed here: the rest of the batch can still have events with this registration as the user data.
        failWatcher(registration);
        return;
    }

    touched_watchers.insert(&registration);
}

#endif

}

#endif
