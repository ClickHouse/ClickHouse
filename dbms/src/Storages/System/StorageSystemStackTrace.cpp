#include <signal.h>

#include <mutex>
#include <condition_variable>
#include <filesystem>

#include <ext/scope_guard.h>

#include <Storages/System/StorageSystemStackTrace.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SIGQUEUE;
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

namespace
{
    struct State
    {
        std::mutex mutex;
        std::condition_variable condvar;

        size_t total_threads;
        size_t threads_processed;
        std::exception_ptr exception;
        MutableColumns * columns_to_fill;

        State() { reset(); }

        void reset(MutableColumns * columns_to_fill_ = nullptr)
        {
            total_threads = 0;
            threads_processed = 0;
            exception = std::exception_ptr();
            columns_to_fill = columns_to_fill_;
        }

        operator bool()
        {
            return columns_to_fill != nullptr;
        }
    };

    State state;

    void callback(const siginfo_t &, const StackTrace & stack_trace, UInt32 thread_number)
    {
        std::lock_guard lock(state.mutex);

        std::cerr << thread_number << " !\n";

        if (!state)
            return;

        try
        {
            size_t stack_trace_size = stack_trace.getSize();
            size_t stack_trace_offset = stack_trace.getOffset();

            Array arr;
            arr.reserve(stack_trace_size - stack_trace_offset);
            for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
                arr.emplace_back(reinterpret_cast<intptr_t>(stack_trace.getFrames()[i]));

            std::cerr << thread_number << " !!\n";

            state.columns_to_fill->at(0)->insert(thread_number);
            state.columns_to_fill->at(1)->insertDefault();
            state.columns_to_fill->at(2)->insert(arr);

            std::cerr << thread_number << " !!!\n";

            ++state.threads_processed;

            std::cerr << state.threads_processed << ", " << state.total_threads << " !!!!\n";
            if (state.threads_processed >= state.total_threads)
                state.condvar.notify_one();
        }
        catch (...)
        {
            state.reset();
            state.exception = std::current_exception();
            state.condvar.notify_one();
        }
    }
}

void StorageSystemStackTrace::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    std::unique_lock lock(state.mutex);

    state.reset(&res_columns);
    SCOPE_EXIT({ state.reset(); });

    std::cerr << state.columns_to_fill->size() << "\n";

    /// Send a signal to every thread
    std::filesystem::directory_iterator end;
    for (std::filesystem::directory_iterator it("/proc/self/task"); it != end; ++it)
    {
        sigval sig_value;
        sig_value.sival_ptr = reinterpret_cast<void *>(&callback);
        pid_t tid = parse<pid_t>(it->path().filename());
        if (0 == ::sigqueue(tid, SIGTSTP, sig_value))
        {
            ++state.total_threads;
        }
        else
        {
            /// The thread may have been already finished.
            if (ESRCH != errno)
                throwFromErrno("Cannot send signal with sigqueue", ErrorCodes::CANNOT_SIGQUEUE);
        }
    }

    std::cerr << state.threads_processed << ", " << state.total_threads << " sent\n";

    /// Timeout one second for the case the signal pipe will be full and messages will be dropped.
    state.condvar.wait_for(lock, std::chrono::seconds(1), []{ return state.threads_processed >= state.total_threads || state.exception; });
    if (state.exception)
        std::rethrow_exception(state.exception);
}

}

