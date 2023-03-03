#ifdef OS_LINUX /// Because of 'sigqueue' functions and RT signals.

#include <signal.h>
#include <poll.h>

#include <mutex>
#include <filesystem>

#include <base/scope_guard.h>

#include <Storages/System/StorageSystemStackTrace.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/PipeFDs.h>
#include <Common/CurrentThread.h>
#include <base/getThreadId.h>
#include <base/logger_useful.h>


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
    // Initialized in StorageSystemStackTrace's ctor and used in signalHandler.
    std::atomic<pid_t> expected_pid;
    const int sig = SIGRTMIN;

    std::atomic<int> sequence_num = 0;    /// For messages sent via pipe.
    std::atomic<int> data_ready_num = 0;
    std::atomic<bool> signal_latch = false;   /// Only need for thread sanitizer.

    /** Notes:
      * Only one query from the table can be processed at the moment of time.
      * This is ensured by the mutex in fillData function.
      * We obtain information about threads by sending signal and receiving info from the signal handler.
      * Information is passed via global variables and pipe is used for signaling.
      * Actually we can send all information via pipe, but we read from it with timeout just in case,
      * so it's convenient to use is only for signaling.
      */

    StackTrace stack_trace{NoCapture{}};

    constexpr size_t max_query_id_size = 128;
    char query_id_data[max_query_id_size];
    size_t query_id_size = 0;

    LazyPipeFDs notification_pipe;

    void signalHandler(int, siginfo_t * info, void * context)
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

        /// In case malicious user is sending signals manually (for unknown reason).
        /// If we don't check - it may break our synchronization.
        if (info->si_pid != expected_pid)
            return;

        /// Signal received too late.
        int notification_num = info->si_value.sival_int;
        if (notification_num != sequence_num.load(std::memory_order_acquire))
            return;

        bool expected = false;
        if (!signal_latch.compare_exchange_strong(expected, true, std::memory_order_acquire))
            return;

        /// All these methods are signal-safe.
        const ucontext_t signal_context = *reinterpret_cast<ucontext_t *>(context);
        stack_trace = StackTrace(signal_context);

        StringRef query_id = CurrentThread::getQueryId();
        query_id_size = std::min(query_id.size, max_query_id_size);
        if (query_id.data && query_id.size)
            memcpy(query_id_data, query_id.data, query_id_size);

        /// This is unneeded (because we synchronize through pipe) but makes TSan happy.
        data_ready_num.store(notification_num, std::memory_order_release);

        ssize_t res = ::write(notification_pipe.fds_rw[1], &notification_num, sizeof(notification_num));

        /// We cannot do anything if write failed.
        (void)res;

        errno = saved_errno;

        signal_latch.store(false, std::memory_order_release);
    }

    /// Wait for data in pipe and read it.
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

            int notification_num = 0;
            ssize_t read_res = ::read(fd, &notification_num, sizeof(notification_num));

            if (read_res < 0)
            {
                if (errno == EINTR)
                    continue;

                throwFromErrno("Cannot read from pipe", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
            }

            if (read_res == sizeof(notification_num))
            {
                if (notification_num == sequence_num.load(std::memory_order_relaxed))
                    return true;
                else
                    continue;   /// Drain delayed notifications.
            }

            throw Exception("Logical error: read wrong number of bytes from pipe", ErrorCodes::LOGICAL_ERROR);
        }
    }
}


StorageSystemStackTrace::StorageSystemStackTrace(const StorageID & table_id_)
    : IStorageSystemOneBlock<StorageSystemStackTrace>(table_id_)
    , log(&Poco::Logger::get("StorageSystemStackTrace"))
{
    notification_pipe.open();

    /// Setup signal handler.
    expected_pid = getpid();
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
        { "thread_name", std::make_shared<DataTypeString>() },
        { "thread_id", std::make_shared<DataTypeUInt64>() },
        { "query_id", std::make_shared<DataTypeString>() },
        { "trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()) }
    };
}


void StorageSystemStackTrace::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
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
        pid_t tid = parse<pid_t>(it->path().filename());

        sigval sig_value{};
        sig_value.sival_int = sequence_num.load(std::memory_order_acquire);
        if (0 != ::sigqueue(tid, sig, sig_value))
        {
            /// The thread may has been already finished.
            if (ESRCH == errno)
                continue;

            throwFromErrno("Cannot send signal with sigqueue", ErrorCodes::CANNOT_SIGQUEUE);
        }

        std::filesystem::path thread_name_path = it->path();
        thread_name_path.append("comm");

        String thread_name;
        if (std::filesystem::exists(thread_name_path))
        {
            constexpr size_t comm_buf_size = 32; /// More than enough for thread name
            ReadBufferFromFile comm(thread_name_path.string(), comm_buf_size);
            readEscapedStringUntilEOL(thread_name, comm);
            comm.close();
        }

        /// Just in case we will wait for pipe with timeout. In case signal didn't get processed.

        if (wait(100) && sig_value.sival_int == data_ready_num.load(std::memory_order_acquire))
        {
            size_t stack_trace_size = stack_trace.getSize();
            size_t stack_trace_offset = stack_trace.getOffset();

            Array arr;
            arr.reserve(stack_trace_size - stack_trace_offset);
            for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
                arr.emplace_back(reinterpret_cast<intptr_t>(stack_trace.getFramePointers()[i]));

            res_columns[0]->insert(thread_name);
            res_columns[1]->insert(tid);
            res_columns[2]->insertData(query_id_data, query_id_size);
            res_columns[3]->insert(arr);
        }
        else
        {
            LOG_DEBUG(log, "Cannot obtain a stack trace for thread {}", tid);

            /// Cannot obtain a stack trace. But create a record in result nevertheless.

            res_columns[0]->insert(thread_name);
            res_columns[1]->insert(tid);
            res_columns[2]->insertDefault();
            res_columns[3]->insertDefault();
        }

        /// Signed integer overflow is undefined behavior in both C and C++. However, according to
        /// C++ standard, Atomic signed integer arithmetic is defined to use two's complement; there
        /// are no undefined results. See https://en.cppreference.com/w/cpp/atomic/atomic and
        /// http://eel.is/c++draft/atomics.types.generic#atomics.types.int-8
        ++sequence_num;
    }
}

}

#endif
