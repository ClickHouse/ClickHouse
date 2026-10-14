#include <Storages/FileLog/DirectoryWatchReactor.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Storages/FileLog/DirectoryWatcherBase.h>

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <optional>
#include <unistd.h>
#if defined(OS_LINUX)
#include <sys/inotify.h>
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
    detachWatcher(watcher, *registration);
}

#if defined(OS_LINUX)

static constexpr size_t buffer_size = 4096;

DirectoryWatchReactor::WatcherRegistration::~WatcherRegistration()
{
    if (fd != -1)
    {
        [[maybe_unused]] int err = ::close(fd);
        chassert(!err || errno == EINTR);
    }
}

DirectoryWatchReactor::DirectoryWatchReactor(std::chrono::milliseconds pairing_timeout_)
    : log(getLogger("DirectoryWatchReactor"))
    , pairing_timeout(pairing_timeout_)
    , epoll(EpollNesting::Leaf)
    , buffer(buffer_size, '\0')
{
    control_pipe.setNonBlockingReadWrite();
    /// The epoll data comes back with every event of the descriptor, so that the loop knows what the event is for.
    /// The control pipe is registered with `this`: an event with `this` as its data is a wake-up for a command,
    /// any other event belongs to the watcher whose registration it carries.
    epoll.add(control_pipe.fds_rw[0], this);

    thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
}

DirectoryWatchReactor::~DirectoryWatchReactor()
{
    stop();
}

void DirectoryWatchReactor::runLoop()
{
    std::array<epoll_event, max_events_per_wait> events{};

    while (true)
    {
        int timeout_ms = -1;
        if (!waiting_for_pairing.empty())
        {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(pairing_deadline - std::chrono::steady_clock::now());
            if (remaining.count() <= 0)
                flushPairingTimeout(); /// Empties the set, so the wait below has no timeout.
            else
                timeout_ms = static_cast<int>(remaining.count());
        }

        const size_t ready = epoll.getManyReady(static_cast<int>(events.size()), events.data(), timeout_ms);

        /// The commands go after the watchers: a command can remove a watcher that has an event in this very batch.
        bool has_commands = false;
        for (size_t i = 0; i < ready; ++i)
        {
            if (events[i].data.ptr == this)
                has_commands = true;
            else
                dispatch(*static_cast<WatcherRegistration *>(events[i].data.ptr));
        }

        if (has_commands)
            processCommands();

        /// Once `stopped` is set no command is submitted, so the queue stays empty and nothing is lost by leaving.
        std::lock_guard lock(mutex);
        if (stopped && commands.empty())
            return;
    }
}

void DirectoryWatchReactor::attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration)
{
    registration->fd = inotify_init1(IN_NONBLOCK);
    if (registration->fd == -1)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot initialize inotify");

    if (inotify_add_watch(registration->fd, watcher.path.c_str(), watcher.watchMask()) == -1)
        ErrnoException::throwFromPath(ErrorCodes::IO_SETUP_ERROR, watcher.path, "Watch directory {} failed", watcher.path);

    epoll.add(registration->fd, registration.get());
    registrations.emplace(&watcher, std::move(registration));
}

void DirectoryWatchReactor::detachWatcher(DirectoryWatcherBase &, WatcherRegistration & registration)
{
    waiting_for_pairing.erase(&registration);
    epoll.remove(registration.fd);
}

void DirectoryWatchReactor::dispatch(WatcherRegistration & registration)
{
    try
    {
        const ssize_t bytes_read = ::read(registration.fd, buffer.data(), buffer.size());
        if (bytes_read < 0)
        {
            /// Nothing to read after all, or interrupted: the descriptor is still readable if there is more.
            if (errno == EAGAIN || errno == EINTR)
                return;
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read the events of a directory watcher");
        }

        if (registration.watcher->onEvents(buffer.data(), static_cast<size_t>(bytes_read)))
        {
            /// Restarted for every waiting watcher, so each of them gets the whole timeout at least but not exactly
            pairing_deadline = std::chrono::steady_clock::now() + pairing_timeout;
            waiting_for_pairing.insert(&registration);
        }
        else
        {
            waiting_for_pairing.erase(&registration);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot process the events of a directory watcher, it is not watched anymore");
        removeWatcher(*registration.watcher);
    }
}

void DirectoryWatchReactor::flushPairingTimeout()
{
    std::unordered_set<WatcherRegistration *> expired;
    expired.swap(waiting_for_pairing);

    for (WatcherRegistration * registration : expired)
    {
        try
        {
            registration->watcher->onPairingTimeout();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot process the events of a directory watcher, it is not watched anymore");
            removeWatcher(*registration->watcher);
        }
    }
}

#elif defined(OS_DARWIN)

DirectoryWatchReactor::DirectoryWatchReactor()
    : log(getLogger("DirectoryWatchReactor"))
{
    kqueue_fd = ::kqueue();
    if (kqueue_fd == -1)
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot create kqueue for the directory watch reactor");

    control_pipe.setNonBlockingReadWrite();
    /// The user data comes back with every event of the descriptor, so that the loop knows what the event is for.
    /// The control pipe is registered with `this`: an event with `this` as its user data is a wake-up for a command,
    /// any other event belongs to the watcher whose registration it carries.
    struct kevent change{};
    /// Edge-triggered, as `drainReadyEvents` must not get the pipe again and again; `commands_pending` remembers it.
    EV_SET(&change, control_pipe.fds_rw[0], EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, this);
    if (::kevent(kqueue_fd, &change, 1, nullptr, 0, nullptr) == -1)
    {
        const int saved_errno = errno;
        int err = ::close(kqueue_fd);
        chassert(!err || errno == EINTR);
        errno = saved_errno;
        throw ErrnoException(ErrorCodes::IO_SETUP_ERROR, "Cannot register the control pipe of the directory watch reactor");
    }

    thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
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

void DirectoryWatchReactor::drainReadyEvents()
{
    std::array<struct kevent, max_events_per_wait> events{};
    while (true)
    {
        const size_t ready = waitForEvents(events, 0, commands_pending);
        if (ready == 0)
            return;
        for (size_t i = 0; i < ready; ++i)
            dispatch(*static_cast<WatcherRegistration *>(events[i].udata), static_cast<int>(events[i].ident), events[i].fflags);
    }
}

void DirectoryWatchReactor::runLoop()
{
    std::array<struct kevent, max_events_per_wait> events{};

    while (true)
    {
        /// Scan the directories of the watchers whose scan is due. This is done between the batches of events, never
        /// while one is being handled: a scan closes and opens descriptors, so an event of the same batch could carry
        /// the number of a descriptor that now belongs to another file.
        /// The due watchers are collected first and then scanned once each, so that a watcher that is due again at
        /// once does not keep the others waiting.
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
        removeFailedWatchers();

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

        bool has_commands = commands_pending;
        commands_pending = false;
        const size_t ready = waitForEvents(events, timeout_ms, has_commands);
        for (size_t i = 0; i < ready; ++i)
            dispatch(*static_cast<WatcherRegistration *>(events[i].udata), static_cast<int>(events[i].ident), events[i].fflags);
        removeFailedWatchers();

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
        registration.failed = true;
        failed_watchers.push_back(registration.watcher);
    }
}

/// The watchers that failed are removed once nothing refers to their registrations anymore.
void DirectoryWatchReactor::removeFailedWatchers()
{
    std::vector<DirectoryWatcherBase *> failed;
    failed.swap(failed_watchers);
    for (DirectoryWatcherBase * watcher : failed)
        removeWatcher(*watcher);
}

void DirectoryWatchReactor::attachWatcher(DirectoryWatcherBase & watcher, std::unique_ptr<WatcherRegistration> registration)
{
    registerVnode(*registration, watcher.directoryFd(), watcher.directoryWatchFlags());
    registrations.emplace(&watcher, std::move(registration));

    /// The first scan of the directory, which takes the baseline and watches its files, is left to the loop: the scan
    /// of a new watcher is due at once, so the loop makes it right after the commands of this batch.
}

void DirectoryWatchReactor::detachWatcher(DirectoryWatcherBase & watcher, WatcherRegistration &)
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
        registration.failed = true;
        failed_watchers.push_back(registration.watcher);
    }
}

#endif

}

#endif
