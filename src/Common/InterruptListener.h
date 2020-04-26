#pragma once

#include <signal.h>
#include <errno.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_WAIT_FOR_SIGNAL;
    extern const int CANNOT_BLOCK_SIGNAL;
    extern const int CANNOT_UNBLOCK_SIGNAL;
}

#ifdef __APPLE__
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
    sigset_t sig_set;

public:
    InterruptListener() : active(false)
    {
        if (sigemptyset(&sig_set)
            || sigaddset(&sig_set, SIGINT))
            throwFromErrno("Cannot manipulate with signal set.", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

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
            else
                throwFromErrno("Cannot poll signal (sigtimedwait).", ErrorCodes::CANNOT_WAIT_FOR_SIGNAL);
        }

        return true;
    }

    void block()
    {
        if (!active)
        {
            if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
                throwFromErrno("Cannot block signal.", ErrorCodes::CANNOT_BLOCK_SIGNAL);

            active = true;
        }
    }

    /// You can stop blocking the signal earlier than in the destructor.
    void unblock()
    {
        if (active)
        {
            if (pthread_sigmask(SIG_UNBLOCK, &sig_set, nullptr))
                throwFromErrno("Cannot unblock signal.", ErrorCodes::CANNOT_UNBLOCK_SIGNAL);

            active = false;
        }
    }
};

}
