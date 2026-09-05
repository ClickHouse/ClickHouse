#pragma once

#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>

#include <atomic>
#else
#include <signal.h>
#include <errno.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_WAIT_FOR_SIGNAL;
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int CANNOT_UNBLOCK_SIGNAL;
}

#if defined(OS_WINDOWS)

/** As long as there exists an object of this class - it takes over Ctrl+C, at the same time it
  * lets you know if it came. This is necessary so that you can interrupt the execution of the
  * request with Ctrl+C.
  * Use only one instance of this class at a time.
  * If `check` method returns true (the interrupt has arrived), the next call will wait for the
  * next one.
  */
class InterruptListener
{
private:
    /// Windows delivers a console control event by calling the handler on a thread of its own
    /// making. There is no per-thread mask to block it with and nothing pending to poll, as there
    /// is for a POSIX signal, so the handler records the event here and `check` consumes it.
    /// Returning nonzero is what suppresses the default action, which is to end the process.
    static inline std::atomic<bool> interrupted{false};

    static BOOL WINAPI handler(DWORD control_type)
    {
        /// Ctrl+Break as well: on Windows it is the other way to interrupt a console program, and
        /// unlike Ctrl+C it cannot be turned into ordinary input, so leaving it alone would end
        /// the process outright.
        if (control_type != CTRL_C_EVENT && control_type != CTRL_BREAK_EVENT)
            return 0;

        interrupted.store(true, std::memory_order_relaxed);
        return 1;
    }

    bool active;

public:
    InterruptListener() : active(false)
    {
        block();
    }

    ~InterruptListener()
    {
        try
        {
            unblock();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
    }

    bool check()
    {
        if (!active)
            return false;

        return interrupted.exchange(false, std::memory_order_relaxed);
    }

    void block()
    {
        if (!active)
        {
            interrupted.store(false, std::memory_order_relaxed);
            if (!SetConsoleCtrlHandler(handler, 1))
                throw Exception(
                    ErrorCodes::CANNOT_BLOCK_SIGNAL, "Cannot install a console control handler, error code: {}", GetLastError());

            active = true;
        }
    }

    /// You can stop taking over Ctrl+C earlier than in the destructor.
    void unblock()
    {
        if (active)
        {
            if (!SetConsoleCtrlHandler(handler, 0))
                throw Exception(
                    ErrorCodes::CANNOT_UNBLOCK_SIGNAL, "Cannot remove the console control handler, error code: {}", GetLastError());

            active = false;
        }
    }
};

#else

#ifdef OS_DARWIN
// We only need to support timeout = {0, 0} at this moment
static int sigtimedwait(const sigset_t *set, siginfo_t *info, const struct timespec * /*timeout*/)
{
    sigset_t pending;
    int signo;
    sigpending(&pending);

    for (signo = 1; signo < NSIG; ++signo)
    {
        if (sigismember(set, signo) && sigismember(&pending, signo))
        {
            sigwait(set, &signo);
            if (info)
            {
                memset(info, 0, sizeof *info);
                info->si_signo = signo;
            }
            return signo;
        }
    }
    errno = EAGAIN;

    return -1;
}
#endif


/** As long as there exists an object of this class - it blocks the INT signal, at the same time it lets you know if it came.
  * This is necessary so that you can interrupt the execution of the request with Ctrl+C.
  * Use only one instance of this class at a time.
  * If `check` method returns true (the signal has arrived), the next call will wait for the next signal.
  */
class InterruptListener
{
private:
    bool active;
    sigset_t sig_set{};

public:
    InterruptListener() : active(false)
    {
        if (sigemptyset(&sig_set) || sigaddset(&sig_set, SIGINT))
            throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Cannot manipulate with signal set");

        block();
    }

    ~InterruptListener()
    {
        unblock();
    }

    bool check()
    {
        if (!active)
            return false;

        timespec timeout = { 0, 0 };

        if (-1 == sigtimedwait(&sig_set, nullptr, &timeout))
        {
            if (errno == EAGAIN)
                return false;
            throw ErrnoException(ErrorCodes::CANNOT_WAIT_FOR_SIGNAL, "Cannot poll signal (sigtimedwait)");
        }

        return true;
    }

    void block()
    {
        if (!active)
        {
            if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
                throw ErrnoException(ErrorCodes::CANNOT_BLOCK_SIGNAL, "Cannot block signal");

            active = true;
        }
    }

    /// You can stop blocking the signal earlier than in the destructor.
    void unblock()
    {
        if (active)
        {
            if (pthread_sigmask(SIG_UNBLOCK, &sig_set, nullptr))
                throw ErrnoException(ErrorCodes::CANNOT_UNBLOCK_SIGNAL, "Cannot unblock signal");

            active = false;
        }
    }
};

#endif

}
