#ifdef OS_LINUX /// Because of 'sigqueue' functions and RT signals.

#include <csignal>
#include <poll.h>

#include <mutex>
#include <filesystem>
#include <unordered_map>

#include <base/scope_guard.h>

#include <Storages/System/StorageSystemStackTrace.h>
#include <Storages/VirtualColumnUtils.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/PipeFDs.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>
#include <Common/logger_useful.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <base/getThreadId.h>


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

    ColumnPtr getFilteredThreadIds(ASTPtr query, ContextPtr context)
    {
        MutableColumnPtr all_thread_ids = ColumnUInt64::create();

        std::filesystem::directory_iterator end;

        /// There is no better way to enumerate threads in a process other than looking into procfs.
        for (std::filesystem::directory_iterator it("/proc/self/task"); it != end; ++it)
        {
            pid_t tid = parse<pid_t>(it->path().filename());
            all_thread_ids->insert(tid);
        }

        Block block { ColumnWithTypeAndName(std::move(all_thread_ids), std::make_shared<DataTypeUInt64>(), "thread_id") };
        VirtualColumnUtils::filterBlockWithQuery(query, block, context);
        return block.getByPosition(0).column;
    }

    using ThreadIdToName = std::unordered_map<UInt64, String, DefaultHash<UInt64>>;
    ThreadIdToName getFilteredThreadNames(ASTPtr query, ContextPtr context, const PaddedPODArray<UInt64> & thread_ids)
    {
        ThreadIdToName tid_to_name;
        MutableColumnPtr all_thread_names = ColumnString::create();

        for (UInt64 tid : thread_ids)
        {
            std::filesystem::path thread_name_path = fmt::format("/proc/self/task/{}/comm", tid);
            String thread_name;
            if (std::filesystem::exists(thread_name_path))
            {
                constexpr size_t comm_buf_size = 32; /// More than enough for thread name
                ReadBufferFromFile comm(thread_name_path.string(), comm_buf_size);
                readEscapedStringUntilEOL(thread_name, comm);
                comm.close();
            }

            tid_to_name[tid] = thread_name;
            all_thread_names->insert(thread_name);
        }

        Block block { ColumnWithTypeAndName(std::move(all_thread_names), std::make_shared<DataTypeString>(), "thread_name") };
        VirtualColumnUtils::filterBlockWithQuery(query, block, context);
        ColumnPtr thread_names = std::move(block.getByPosition(0).column);

        std::unordered_set<String> filtered_thread_names;
        for (size_t i = 0; i != thread_names->size(); ++i)
        {
            const auto & thread_name = thread_names->getDataAt(i);
            filtered_thread_names.emplace(thread_name);
        }

        for (const auto & [tid, name] : tid_to_name)
        {
            if (!filtered_thread_names.contains(name))
                tid_to_name.erase(tid);
        }

        return tid_to_name;
    }
}


StorageSystemStackTrace::StorageSystemStackTrace(const StorageID & table_id_)
    : IStorage(table_id_)
    , log(&Poco::Logger::get("StorageSystemStackTrace"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        { "thread_name", std::make_shared<DataTypeString>() },
        { "thread_id", std::make_shared<DataTypeUInt64>() },
        { "query_id", std::make_shared<DataTypeString>() },
        { "trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()) },
    }, { /* aliases */ }));
    setInMemoryMetadata(storage_metadata);

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


Pipe StorageSystemStackTrace::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    /// It shouldn't be possible to do concurrent reads from this table.
    std::lock_guard lock(mutex);

    /// Create a mask of what columns are needed in the result.

    NameSet names_set(column_names.begin(), column_names.end());

    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    std::vector<UInt8> columns_mask(sample_block.columns());
    for (size_t i = 0, size = columns_mask.size(); i < size; ++i)
    {
        if (names_set.contains(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
        }
    }

    bool send_signal = names_set.contains("trace") || names_set.contains("query_id");
    bool read_thread_names = names_set.contains("thread_name");

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    /// Send a signal to every thread and wait for result.
    /// We must wait for every thread one by one sequentially,
    ///  because there is a limit on number of queued signals in OS and otherwise signals may get lost.
    /// Also, non-RT signals are not delivered if previous signal is handled right now (by default; but we use RT signals).

    /// Obviously, results for different threads may be out of sync.

    ColumnPtr thread_ids = getFilteredThreadIds(query_info.query, context);
    const auto & thread_ids_data = assert_cast<const ColumnUInt64 &>(*thread_ids).getData();

    ThreadIdToName thread_names;
    if (read_thread_names)
        thread_names = getFilteredThreadNames(query_info.query, context, thread_ids_data);

    for (UInt64 tid : thread_ids_data)
    {
        size_t res_index = 0;

        String thread_name;
        if (auto it = thread_names.find(tid); it != thread_names.end())
            thread_name = it->second;

        if (!send_signal)
        {
            res_columns[res_index++]->insert(thread_name);
            res_columns[res_index++]->insert(tid);
            res_columns[res_index++]->insertDefault();
            res_columns[res_index++]->insertDefault();
        }
        else
        {
            sigval sig_value{};

            sig_value.sival_int = sequence_num.load(std::memory_order_acquire);
            if (0 != ::sigqueue(tid, sig, sig_value))
            {
                /// The thread may has been already finished.
                if (ESRCH == errno)
                    continue;

                throwFromErrno("Cannot send signal with sigqueue", ErrorCodes::CANNOT_SIGQUEUE);
            }

            /// Just in case we will wait for pipe with timeout. In case signal didn't get processed.
            if (send_signal && wait(100) && sig_value.sival_int == data_ready_num.load(std::memory_order_acquire))
            {
                size_t stack_trace_size = stack_trace.getSize();
                size_t stack_trace_offset = stack_trace.getOffset();

                Array arr;
                arr.reserve(stack_trace_size - stack_trace_offset);
                for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
                    arr.emplace_back(reinterpret_cast<intptr_t>(stack_trace.getFramePointers()[i]));

                res_columns[res_index++]->insert(thread_name);
                res_columns[res_index++]->insert(tid);
                res_columns[res_index++]->insertData(query_id_data, query_id_size);
                res_columns[res_index++]->insert(arr);
            }
            else
            {
                LOG_DEBUG(log, "Cannot obtain a stack trace for thread {}", tid);

                res_columns[res_index++]->insert(thread_name);
                res_columns[res_index++]->insert(tid);
                res_columns[res_index++]->insertDefault();
                res_columns[res_index++]->insertDefault();
            }

            /// Signed integer overflow is undefined behavior in both C and C++. However, according to
            /// C++ standard, Atomic signed integer arithmetic is defined to use two's complement; there
            /// are no undefined results. See https://en.cppreference.com/w/cpp/atomic/atomic and
            /// http://eel.is/c++draft/atomics.types.generic#atomics.types.int-8
            ++sequence_num;
        }
    }

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(sample_block, std::move(chunk)));
}

}

#endif
