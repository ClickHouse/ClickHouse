#include <signal.h>
#include <poll.h>

#include <mutex>
#include <filesystem>

#include <ext/scope_guard.h>

#include <Storages/System/StorageSystemStackTrace.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <Common/PipeFDs.h>
#include <common/getThreadNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SIGQUEUE;
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int LOGICAL_ERROR;
}


namespace
{
    const pid_t expected_pid = getpid();
    const int sig = SIGRTMIN;
    UInt32 thread_number{0};
    std::optional<StackTrace> stack_trace;
    LazyPipeFDs notification_pipe;

    void signalHandler(int, siginfo_t * info, void * context)
    {
        /// In case malicious user is sending signals manually (for unknown reason).
        /// If we don't check - it may break our synchronization.
        if (info->si_pid != expected_pid)
            return;

        /// All these methods are signal-safe.
        const ucontext_t signal_context = *reinterpret_cast<ucontext_t *>(context);
        stack_trace.emplace(signal_context);
        thread_number = getThreadNumber();

        char buf = 0;
        /// We cannot do anything if write failed.
        (void)::write(notification_pipe.fds_rw[1], &buf, 1);
    }

    /// Wait for data in pipe.
    bool wait(int timeout_ms)
    {
        while (true)
        {
            int fd = notification_pipe.fds_rw[0];
            pollfd poll_fd{fd, POLLIN, 0};

            int poll_res = poll(&poll_fd, 1, timeout_ms);
            if (poll_res < 0)
            {
                if (errno == EINTR)
                {
                    --timeout_ms;   /// Quite a hacky way to update timeout. Just to make sure we avoid infinite waiting.
                    if (timeout_ms == 0)
                        return false;
                    continue;
                }

                throwFromErrno("Cannot poll pipe", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
            }
            if (poll_res == 0)
                return false;

            char buf = 0;
            ssize_t read_res = ::read(fd, &buf, 1);
            if (read_res == 1)
                return true;

            if (read_res < 0)
            {
                if (errno == EINTR)
                    continue;

                throwFromErrno("Cannot read from pipe", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
            }

            throw Exception("Logical error: read for one byte returned more than one byte", ErrorCodes::LOGICAL_ERROR);
        }
    }
}


StorageSystemStackTrace::StorageSystemStackTrace(const String & name)
    : IStorageSystemOneBlock<StorageSystemStackTrace>(name)
{
    notification_pipe.open();

    /// Setup signal handler.

    struct sigaction sa{};
    sa.sa_sigaction = signalHandler;
    sa.sa_flags = SA_SIGINFO;

    if (sigemptyset(&sa.sa_mask))
        throwFromErrno("Cannot set signal handler.", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaddset(&sa.sa_mask, sig))
        throwFromErrno("Cannot set signal handler.", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaction(sig, &sa, nullptr))
        throwFromErrno("Cannot set signal handler.", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);
}


NamesAndTypesList StorageSystemStackTrace::getNamesAndTypes()
{
    return
    {
        { "thread_number", std::make_shared<DataTypeUInt32>() },
        { "query_id", std::make_shared<DataTypeString>() },
        { "trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()) }
    };
}


void StorageSystemStackTrace::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    /// It shouldn't be possible to do concurrent reads from this table.
    std::lock_guard lock(mutex);

    /// Send a signal to every thread and wait for result.
    /// We must wait for every thread one by one sequentially,
    ///  because there is a limit on number of queued signals in OS and otherwise signals may get lost.
    /// Also, non-RT signals are not delivered if previous signal is handled right now (by default; but we use RT signals).

    /// Obviously, results for different threads may be out of sync.

    /// There is no better way to enumerate threads in a process other than looking into procfs.

    std::filesystem::directory_iterator end;
    for (std::filesystem::directory_iterator it("/proc/self/task"); it != end; ++it)
    {
        sigval sig_value{};
        pid_t tid = parse<pid_t>(it->path().filename());

        std::cerr << "Requested: " << tid << "\n";

        if (0 != ::sigqueue(tid, sig, sig_value))
        {
            /// The thread may has been already finished.
            if (ESRCH == errno)
                continue;

            throwFromErrno("Cannot send signal with sigqueue", ErrorCodes::CANNOT_SIGQUEUE);
        }

        /// Just in case we will wait for pipe with timeout. In case signal didn't get processed.
        if (wait(100))
        {
            size_t stack_trace_size = stack_trace->getSize();
            size_t stack_trace_offset = stack_trace->getOffset();

            Array arr;
            arr.reserve(stack_trace_size - stack_trace_offset);
            for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
                arr.emplace_back(reinterpret_cast<intptr_t>(stack_trace->getFrames()[i]));

            std::cerr << tid << ", " << thread_number << " !!\n";

            res_columns[0]->insert(thread_number);
            res_columns[1]->insertDefault();
            res_columns[2]->insert(arr);
        }
        else
        {
            /// Cannot obtain a stack trace. But create a record in result nevertheless.

            res_columns[0]->insert(tid);
            res_columns[1]->insertDefault();
            res_columns[2]->insertDefault();
        }
    }
}

}

