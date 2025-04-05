#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

#include <span>
#include <Common/PODArray.h>

namespace DB
{
class ReadBuffer;
class SeekableReadBuffer;
}

namespace DB::Parquet
{

class Prefetcher
{
private:
    struct RequestState;
    struct Task;

public:
    /// Pins a pre-registered range that we may want to read.
    /// Call reset to mark the range as no longer needed and subtract its memory usage from MemoryUsageDiff.
    /// All handles must be destroyed before Prefetcher is destroyed.
    class RequestHandle
    {
    public:
        RequestHandle() = default;
        RequestHandle(RequestHandle &&) noexcept;
        RequestHandle & operator=(RequestHandle &&) noexcept;

        /// Doesn't record deallocated memory in MemoryUsageDiff. Should only be called on shutdown,
        /// otherwise use reset(diff).
        ~RequestHandle();

        operator bool() const { return request != nullptr; }

        void reset(MemoryUsageDiff * diff);

    private:
        friend class Prefetcher;

        RequestState * request = nullptr;
        MemoryUsageToken memory;

        RequestHandle(RequestState * request_, MemoryUsageToken memory_);
    };

    void init(ReadBuffer * reader_, const ReadOptions & options, SharedParsingThreadPoolPtr thread_pool_);

    /// Waits for in-progress reads to complete, cancels queued reads that haven't started yet.
    ~Prefetcher();

    /// Not thread safe.
    /// All ranges must be registered before any reading happens (except direct readSync).
    /// Ranges are allowed to overlap a little, but this decreases the effectiveness of range
    /// coalescing, and the overlap might be read from file multiple times.
    /// If likely_to_be_used is true, Prefetcher will be more eager to piggy-back this range when
    /// reading other ranges.
    RequestHandle registerRange(size_t offset, size_t length, bool likely_to_be_used);

    /// Called at most once, after all registerRange calls and before all enqueue/getRangeData calls.
    void finalizeRanges();

    /// Kicks off background tasks to prefetch these range, if needed (if not already started, and
    /// prefetching is enabled, and handles are nonempty).
    /// Adds the range's memory usage to MemoryUsageDiff. Remembers memory_usage->stage so that
    /// RequestHandle::reset can later subtract from MemoryUsageDiff correctly.
    /// The memory usage may be higher than RequestHandle::length() if we took on some read
    /// amplification to avoid seeks.
    void startPrefetch(const std::vector<RequestHandle *> & requests_to_start, MemoryUsageDiff * diff);

    /// The input `request` must come from registerRange, not from another splitAndEnqueueRange.
    /// I.e. ranges can't be split more than once.
    /// `subranges` must be sorted.
    void splitAndPrefetchRange(
        RequestHandle request, const std::vector<std::pair</*global_offset*/ size_t, /*length*/ size_t>> & subranges, std::vector<RequestHandle> & out_handles, MemoryUsageDiff * diff);

    /// If prefetched, returns prefetched data.
    /// If prefetch in progress, waits for it to complete.
    /// If prefetch not started, reads the data right here.
    /// The returned pointer is valid as long as the RequestHandle is alive.
    std::span<const char> getRangeData(const RequestHandle & request);

    /// Pass-through read from the underlying ReadBuffer.
    void readSync(char * to, size_t n, size_t offset);

    size_t getFileSize() const { return file_size; }

private:
    /// Corresponds to RequestHandle.
    struct RequestState
    {
        /// State transitions:
        ///
        /// HasRange -> HasTask
        ///       |      |
        ///       v      v
        ///       Cancelled
        ///
        /// Transition to HasTask happen with `mutex` locked, after assigning `task` and `task_offset`.
        enum class State
        {
            HasRange,
            HasTask,
            Cancelled, // RequestHandle was reset
        };

        std::atomic<State> state {State::HasRange};

        /// Whether this range can be piggy-backed to nearby other reads.
        std::atomic<bool> allow_incidental_read {true};

        union
        {
            size_t range_idx = UINT64_MAX; // if HasRange; read with locked `mutex`
            Task * task; // if HasTask
        } range_or_task;

        size_t length = 0;
        size_t task_offset = 0;
    };

    /// Range that the user wants, before coalescing. Overlapping ranges are allowed, but are not
    /// handled optimally and should be avoided when possible.
    struct RangeState
    {
        RequestState * request;

        size_t start;
        size_t end;

        size_t length() const { return end - start; }
    };

    /// A range to read from file. May cover multiple request ranges.
    /// Tasks' ranges may overlap (if requested ranges overlap).
    struct Task
    {
        enum class State : UInt8
        {
            Scheduled,
            Running,
            Done,
            Exception,
            /// This range is no longer needed, `buf` can be deallocated.
            /// Task may still be running; in this case, the runner will deallocate `buf` when done.
            Deallocated,
        };

        size_t offset;
        size_t length;
        double memory_amplification = 1;

        PaddedPODArray<char> buf;

        std::atomic<State> state {State::Scheduled};
        /// How many RequestState-s in HasTask state point to this Task.
        std::atomic<size_t> refcount {};
        /// Notified when the state changes from Running to Done or Exception.
        CompletionNotification completion;
        std::exception_ptr exception;
    };

    enum class ReadMode
    {
        /// The normal mode: use reader->readBigAt, no read_mutex.
        RandomRead,
        /// Slow mode: use reader->seek and reader->next with read_mutex.
        SeekAndRead,
        /// The whole file was read into `entire_file`, no further reading required.
        EntireFileIsInMemory,
    };

    SharedParsingThreadPoolPtr thread_pool;

    std::mutex read_mutex;
    ReadMode read_mode;
    SeekableReadBuffer * reader = nullptr;
    PaddedPODArray<char> entire_file;

    size_t file_size;
    size_t min_bytes_for_seek;
    size_t bytes_per_read_task;

    std::shared_ptr<ShutdownHelper> shutdown = std::make_shared<ShutdownHelper>();

    /// Locked when creating a Task.
    std::mutex mutex;

    //TODO: TSA
    /// Arena for user requests.
    std::deque<RequestState> requests;
    /// Arena for read tasks.
    std::deque<Task> tasks;
    /// Pre-registered ranges. Sorted and immutable after finalizeRanges().
    std::vector<RangeState> ranges;

    std::atomic<bool> ranges_finalized {false};

    /// (One mutex for all tasks because it's not used frequently.)
    std::mutex exception_mutex;//TODO: TSA

    void determineReadModeAndFileSize(ReadBuffer * reader_, const ReadOptions & options);
    /// Creates and starts a Task covering this request and possibly other nearby ranges.
    ///
    /// If splitting, the request is being cancelled and replaced by a smaller range
    /// (splitAndPrefetchRange), and only subrange [subrange_start, subrange_end) needs to be read.
    /// In this case, the RequestState will not transition to HasTask state, and the returned task
    /// will have refcount incremented (to prevent Task from being deallocated before the caller
    /// has a chance to create RequestState-s pointing to it).
    Task * pickRangesAndCreateTask(RequestState *, const RequestHandle &, bool splitting, size_t subrange_start, size_t subrange_end);
    static void decreaseTaskRefcount(Task * task, size_t amount);
    void scheduleTask(Task * task);
    Task::State runTask(Task * task);
    [[noreturn]] void rethrowException(Task * task);
};

}
