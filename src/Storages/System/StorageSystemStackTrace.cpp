#ifdef OS_LINUX /// Because of 'rt_tgsigqueueinfo' functions and RT signals.

#include <poll.h>

#include <mutex>
#include <filesystem>
#include <unordered_map>
#include <memory>

#include <base/scope_guard.h>

#include <Storages/System/StorageSystemStackTrace.h>
#include <Storages/VirtualColumnUtils.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/PipeFDs.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/getThreadId.h>
#include <sys/syscall.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds storage_system_stack_trace_pipe_read_timeout_ms;
}

namespace ErrorCodes
{
    extern const int CANNOT_SIGQUEUE;
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}


namespace
{

// Initialized in StorageSystemStackTrace's ctor and used in signalHandler.
std::atomic<pid_t> server_pid;
const int STACK_TRACE_SERVICE_SIGNAL = SIGRTMIN;

std::atomic<int> sequence_num = 0;    /// For messages sent via pipe.
std::atomic<int> data_ready_num = 0;
std::atomic<bool> signal_latch = false;   /// Only need for thread sanitizer.

/** Notes:
  * Only one query from the table can be processed at the moment of time.
  * This is ensured by the mutex in StorageSystemStackTraceSource.
  * We obtain information about threads by sending signal and receiving info from the signal handler.
  * Information is passed via global variables and pipe is used for signaling.
  * Actually we can send all information via pipe, but we read from it with timeout just in case,
  * so it's convenient to use it only for signaling.
  */
std::mutex mutex;

StackTrace stack_trace{NoCapture{}};

constexpr size_t max_query_id_size = 128;
char query_id_data[max_query_id_size];
size_t query_id_size = 0;

LazyPipeFDs notification_pipe;

int rt_tgsigqueueinfo(pid_t tgid, pid_t tid, int sig, siginfo_t *info)
{
    return static_cast<int>(syscall(__NR_rt_tgsigqueueinfo, tgid, tid, sig, info));
}

void signalHandler(int, siginfo_t * info, void * context)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    /// In case malicious user is sending signals manually (for unknown reason).
    /// If we don't check - it may break our synchronization.
    if (info->si_pid != server_pid)
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

    auto query_id = CurrentThread::getQueryId();
    query_id_size = std::min(query_id.size(), max_query_id_size);
    if (!query_id.empty())
        memcpy(query_id_data, query_id.data(), query_id_size);

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

            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot poll pipe");
        }
        if (poll_res == 0)
            return false;

        int notification_num = 0;
        ssize_t read_res = ::read(fd, &notification_num, sizeof(notification_num));

        if (read_res < 0)
        {
            if (errno == EINTR)
                continue;

            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from pipe");
        }

        if (read_res == sizeof(notification_num))
        {
            if (notification_num == sequence_num.load(std::memory_order_relaxed))
                return true;
            continue; /// Drain delayed notifications.
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Read wrong number of bytes from pipe");
    }
}

using ThreadIdToName = std::unordered_map<UInt64, String, DefaultHash<UInt64>>;
ThreadIdToName getFilteredThreadNames(const ActionsDAG::Node * predicate, ContextPtr context, const PaddedPODArray<UInt64> & thread_ids, LoggerPtr log)
{
    ThreadIdToName tid_to_name;
    MutableColumnPtr all_thread_names = ColumnString::create();

    Stopwatch watch;
    for (UInt64 tid : thread_ids)
    {
        String thread_name;
        constexpr size_t comm_buf_size = 32; /// More than enough for thread name

        try
        {
            ReadBufferFromFile comm(fmt::format("/proc/self/task/{}/comm", tid), comm_buf_size);
            readEscapedStringUntilEOL(thread_name, comm);
            comm.close();
        }
        catch (const Exception & e)
        {
            /// Ignore TOCTOU error
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
                continue;
            throw;
        }

        tid_to_name[tid] = thread_name;
        all_thread_names->insert(thread_name);
    }
    LOG_TRACE(log, "Read {} thread names for {} threads, took {} ms", tid_to_name.size(), thread_ids.size(), watch.elapsedMilliseconds());

    Block block { ColumnWithTypeAndName(std::move(all_thread_names), std::make_shared<DataTypeString>(), "thread_name") };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    ColumnPtr thread_names = std::move(block.getByPosition(0).column);

    std::unordered_set<String> filtered_thread_names;
    for (size_t i = 0; i != thread_names->size(); ++i)
    {
        const auto & thread_name = thread_names->getDataAt(i);
        filtered_thread_names.emplace(thread_name);
    }

    for (auto it = tid_to_name.begin(); it != tid_to_name.end();)
    {
        if (!filtered_thread_names.contains(it->second))
            it = tid_to_name.erase(it);
        else
            ++it;
    }

    return tid_to_name;
}

bool parseHexNumber(std::string_view sv, UInt64 & res)
{
    errno = 0; /// Functions strto* don't clear errno.
    char * pos_integer = const_cast<char *>(sv.data());
    res = std::strtoull(sv.data(), &pos_integer, 16); /// NOLINT(bugprone-suspicious-stringview-data-usage)
    return (pos_integer == sv.data() + sv.size() && errno != ERANGE);
}

bool isSignalBlocked(UInt64 tid, int signal)
{
    String buffer;

    try
    {
        ReadBufferFromFile status(fmt::format("/proc/{}/status", tid));
        while (!status.eof())
        {
            readEscapedStringUntilEOL(buffer, status);
            if (!status.eof())
                ++status.position();
            if (buffer.starts_with("SigBlk:"))
                break;
        }
        status.close();

        std::string_view line(buffer);
        line = line.substr(strlen("SigBlk:"));
        line = line.substr(0, line.rend() - std::find_if_not(line.rbegin(), line.rend(), ::isspace));

        UInt64 sig_blk;
        if (parseHexNumber(line, sig_blk))
            return sig_blk & signal;
    }
    catch (const Exception & e)
    {
        /// Ignore TOCTOU error
        if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
            throw;
    }

    return false;
}

/// Send a signal to every thread and wait for result.
/// We must wait for every thread one by one sequentially,
///  because there is a limit on number of queued signals in OS and otherwise signals may get lost.
/// Also, non-RT signals are not delivered if previous signal is handled right now (by default; but we use RT signals).
class StackTraceSource : public ISource
{
public:
    StackTraceSource(
        const Names & column_names,
        Block header_,
        std::shared_ptr<const ActionsDAG> filter_dag_,
        ContextPtr context_,
        UInt64 max_block_size_,
        LoggerPtr log_)
        : ISource(header_)
        , context(context_)
        , header(std::move(header_))
        , filter_dag(std::move(filter_dag_))
        , predicate(filter_dag ? filter_dag->getOutputs().at(0) : nullptr)
        , max_block_size(max_block_size_)
        , pipe_read_timeout_ms(
              static_cast<int>(context->getSettingsRef()[Setting::storage_system_stack_trace_pipe_read_timeout_ms].totalMilliseconds()))
        , log(log_)
        , proc_it("/proc/self/task")
        /// It shouldn't be possible to do concurrent reads from this table.
        , lock(mutex)
        , signal_str(strsignal(STACK_TRACE_SERVICE_SIGNAL)) /// NOLINT(concurrency-mt-unsafe) // not thread-safe but ok in this context
    {
        /// Create a mask of what columns are needed in the result.
        NameSet names_set(column_names.begin(), column_names.end());
        send_signal = names_set.contains("trace") || names_set.contains("query_id");
        read_thread_names = names_set.contains("thread_name");
    }

    String getName() const override { return "StackTrace"; }

protected:
    Chunk generate() override
    {
        MutableColumns res_columns = header.cloneEmptyColumns();

        ColumnPtr thread_ids;
        {
            Stopwatch watch;
            thread_ids = getFilteredThreadIds();
            LOG_TRACE(log, "Read {} threads, took {} ms", thread_ids->size(), watch.elapsedMilliseconds());
        }
        if (thread_ids->empty())
            return Chunk();

        const auto & thread_ids_data = assert_cast<const ColumnUInt64 &>(*thread_ids).getData();

        /// NOTE: This is racy, so you may get incorrect thread_name.
        ThreadIdToName thread_names;
        if (read_thread_names)
            thread_names = getFilteredThreadNames(predicate, context, thread_ids_data, log);

        for (UInt64 tid : thread_ids_data)
        {
            size_t res_index = 0;

            String thread_name;
            if (read_thread_names)
            {
                if (auto it = thread_names.find(tid); it != thread_names.end())
                    thread_name = it->second;
                else
                    continue; /// was filtered out by "thread_name" condition
            }

            if (!send_signal)
            {
                res_columns[res_index++]->insert(thread_name);
                res_columns[res_index++]->insert(tid);
                res_columns[res_index++]->insertDefault();
                res_columns[res_index++]->insertDefault();
            }
            else
            {
                /// NOTE: This check is racy (thread can be
                /// destroyed/replaced/...), but it is OK, since only the
                /// following could happen:
                /// - it will incorrectly detect that the signal is blocked and
                ///   will not send it this time
                /// - it will incorrectly detect that the signal is not blocked
                ///   then it will wait storage_system_stack_trace_pipe_read_timeout_ms
                bool signal_blocked = isSignalBlocked(tid, STACK_TRACE_SERVICE_SIGNAL);
                if (!signal_blocked)
                {
                    ++signals_sent;
                    Stopwatch watch;
                    SCOPE_EXIT({
                        signals_sent_ms += watch.elapsedMilliseconds();

                        /// Signed integer overflow is undefined behavior in both C and C++. However, according to
                        /// C++ standard, Atomic signed integer arithmetic is defined to use two's complement; there
                        /// are no undefined results. See https://en.cppreference.com/w/cpp/atomic/atomic and
                        /// http://eel.is/c++draft/atomics.types.generic#atomics.types.int-8
                        ++sequence_num;
                    });

                    siginfo_t sig_info{};
                    sig_info.si_code = SI_QUEUE; /// sigqueue()
                    sig_info.si_pid = server_pid;
                    sig_info.si_value.sival_int = sequence_num.load(std::memory_order_acquire);

                    if (0 != rt_tgsigqueueinfo(server_pid, static_cast<pid_t>(tid), STACK_TRACE_SERVICE_SIGNAL, &sig_info))
                    {
                        /// The thread may has been already finished.
                        if (ESRCH == errno)
                            continue;

                        throw ErrnoException(ErrorCodes::CANNOT_SIGQUEUE, "Cannot queue a signal");
                    }

                    /// Just in case we will wait for pipe with timeout. In case signal didn't get processed.
                    if (wait(pipe_read_timeout_ms) && sig_info.si_value.sival_int == data_ready_num.load(std::memory_order_acquire))
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

                        continue;
                    }
                }

                if (signal_blocked)
                    LOG_DEBUG(log, "Thread {} ({}) blocks SIG{} signal", tid, thread_name, signal_str);
                else
                    LOG_DEBUG(log, "Cannot obtain a stack trace for thread {} ({})", tid, thread_name);

                res_columns[res_index++]->insert(thread_name);
                res_columns[res_index++]->insert(tid);
                res_columns[res_index++]->insertDefault();
                res_columns[res_index++]->insertDefault();
            }
        }
        LOG_TRACE(log, "Send signal to {} threads (total), took {} ms", signals_sent, signals_sent_ms);

        UInt64 num_rows = res_columns.at(0)->size();
        Chunk chunk(std::move(res_columns), num_rows);
        return chunk;
    }

private:
    ContextPtr context;
    Block header;
    const std::shared_ptr<const ActionsDAG> filter_dag;
    const ActionsDAG::Node * predicate;

    const size_t max_block_size;
    const int pipe_read_timeout_ms;
    bool send_signal = false;
    bool read_thread_names = false;

    LoggerPtr log;

    std::filesystem::directory_iterator proc_it;
    std::filesystem::directory_iterator end;

    size_t signals_sent = 0;
    size_t signals_sent_ms = 0;

    std::unique_lock<std::mutex> lock;
    const char * signal_str;

    ColumnPtr getFilteredThreadIds()
    {
        MutableColumnPtr all_thread_ids = ColumnUInt64::create();

        size_t i = 0;
        /// There is no better way to enumerate threads in a process other than looking into procfs.
        for (; i < max_block_size && proc_it != end; ++proc_it, ++i)
        {
            pid_t tid = parse<pid_t>(proc_it->path().filename());
            all_thread_ids->insert(tid);
        }

        Block block { ColumnWithTypeAndName(std::move(all_thread_ids), std::make_shared<DataTypeUInt64>(), "thread_id") };
        VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);

        return block.getByPosition(0).column;
    }
};

class ReadFromSystemStackTrace : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromSystemStackTrace"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        Pipe pipe(std::make_shared<StackTraceSource>(
            column_names,
            getOutputHeader(),
            filter_actions_dag,
            context,
            max_block_size,
            log));
        pipeline.init(std::move(pipe));
    }

    ReadFromSystemStackTrace(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        size_t max_block_size_,
        LoggerPtr log_)
        : SourceStepWithFilter(std::move(sample_block), column_names_, query_info_, storage_snapshot_, context_)
        , column_names(column_names_)
        , max_block_size(max_block_size_)
        , log(log_)
    {
    }

private:
    Names column_names;
    size_t max_block_size;
    LoggerPtr log;
};

}


StorageSystemStackTrace::StorageSystemStackTrace(const StorageID & table_id_)
    : IStorage(table_id_)
    , log(getLogger("StorageSystemStackTrace"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({
        {"thread_name", std::make_shared<DataTypeString>(), "The name of the thread."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "The thread identifier"},
        {"query_id", std::make_shared<DataTypeString>(), "The ID of the query this thread belongs to."},
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "The stacktrace of this thread. Basically just an array of addresses."},
    }));
    setInMemoryMetadata(storage_metadata);

    notification_pipe.open();

    /// Setup signal handler.
    server_pid = getpid();
    struct sigaction sa{};
    sa.sa_sigaction = signalHandler;
    sa.sa_flags = SA_SIGINFO;

    if (sigemptyset(&sa.sa_mask))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Cannot set signal handler");

    if (sigaddset(&sa.sa_mask, STACK_TRACE_SERVICE_SIGNAL))
        throw ErrnoException(ErrorCodes::CANNOT_MANIPULATE_SIGSET, "Cannot set signal handler");

    if (sigaction(STACK_TRACE_SERVICE_SIGNAL, &sa, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler");
}


void StorageSystemStackTrace::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto reading = std::make_unique<ReadFromSystemStackTrace>(
        column_names, query_info, storage_snapshot, context, sample_block, max_block_size, log);
    query_plan.addStep(std::move(reading));
}

}

#endif
