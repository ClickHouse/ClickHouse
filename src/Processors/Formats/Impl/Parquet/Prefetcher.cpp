#include <Processors/Formats/Impl/Parquet/Prefetcher.h>

#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/copyData.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace DB::Parquet
{

void Prefetcher::init(ReadBuffer * reader_, const ReadOptions & options, SharedParsingThreadPoolPtr thread_pool_)
{
    min_bytes_for_seek = options.min_bytes_for_seek;
    bytes_per_read_task = options.bytes_per_read_task;
    thread_pool = thread_pool_;
    determineReadModeAndFileSize(reader_, options);
}

Prefetcher::~Prefetcher()
{
    shutdown->shutdown();
}

void Prefetcher::determineReadModeAndFileSize(ReadBuffer * reader_, const ReadOptions & options)
{
    if (options.seekable_read)
    {
        bool has_file_size = isBufferWithFileSize(*reader_);
        auto * seekable = dynamic_cast<SeekableReadBuffer *>(reader_);
        if (has_file_size && seekable)
        {
            if (seekable->supportsReadAt())
            {
                reader = seekable;
                read_mode = ReadMode::RandomRead;
            }
            else if (seekable->checkIfActuallySeekable())
            {
                reader = seekable;
                read_mode = ReadMode::SeekAndRead;
            }

            if (reader)
                file_size = getFileSizeFromReadBuffer(*seekable);
        }
    }

    if (!reader)
    {
        /// Avoid loading the whole file if it's clearly not a parquet file.
        constexpr std::string_view expected_prefix = "PAR1";
        if (!reader_->eof() && reader_->available() >= expected_prefix.size() &&
            memcmp(reader_->position(), expected_prefix.data(), expected_prefix.size()) != 0)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not a parquet file (wrong magic bytes at the start)");
        }

        WriteBufferFromVector<PaddedPODArray<char>> out(entire_file);
        copyData(*reader_, out);
        out.finalize();

        read_mode = ReadMode::EntireFileIsInMemory;
        file_size = entire_file.size();
    }
}

void Prefetcher::readSync(char * to, size_t n, size_t offset)
{
    if (offset > file_size || n > file_size - offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File read out of bounds: offset {}, length {}, file size {}", offset, n, file_size);

    size_t nread = 0;
    switch (read_mode)
    {
        case ReadMode::RandomRead:
            nread = reader->readBigAt(to, n, offset, /*progress_callback*/ nullptr);
            break;
        case ReadMode::SeekAndRead:
        {
            std::lock_guard lock(read_mutex);
            // Seeking to a position above a previous setReadUntilPosition() confuses some of the
            // ReadBuffer implementations.
            reader->setReadUntilEnd();
            reader->seek(offset, SEEK_SET);
            reader->setReadUntilPosition(offset + n);
            nread = reader->readBig(to, n);
            break;
        }
        case ReadMode::EntireFileIsInMemory:
            memcpy(to, entire_file.data() + offset, n);
            nread = n;
            break;
    }
    if (nread != n)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected eof: offset {}, length {}, bytes read {}, expected file size {}", offset, n, nread, file_size);
}

Prefetcher::RequestHandle Prefetcher::registerRange(size_t offset, size_t length, bool likely_to_be_used)
{
    chassert(!ranges_finalized.load(std::memory_order_relaxed));
    if (offset > file_size || length > file_size - offset)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Range out of bounds: offset {}, length {}, file size {}", offset, length, file_size);
    RequestState & req = requests.emplace_back();
    req.length = length;
    req.allow_incidental_read.store(likely_to_be_used || length < min_bytes_for_seek, std::memory_order_relaxed);
    ranges.push_back(RangeState {.request = &req, .start = offset, .end = offset + length});
    return RequestHandle(&req, MemoryUsageToken());
}

void Prefetcher::finalizeRanges()
{
    bool already_finalized = ranges_finalized.exchange(true);
    chassert(!already_finalized);
    std::sort(ranges.begin(), ranges.end(), [](const RangeState & a, const RangeState & b)
        {
            return a.start < b.start;
        });
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        RequestState * req = ranges[i].request;
        const auto s = req->state.load(std::memory_order_relaxed);
        if (s == RequestState::State::HasRange)
            req->range_or_task.range_idx = i;
        else
            chassert(s == RequestState::State::Cancelled);
    }
}

void Prefetcher::startPrefetch(const std::vector<RequestHandle *> & requests_to_start, MemoryUsageDiff * diff)
{
    chassert(ranges_finalized.load(std::memory_order_relaxed));

    /// Allow the requested ranges can be coalesced with each other even if they're longer than
    /// min_bytes_for_seek.
    for (const RequestHandle * handle : requests_to_start)
        if (*handle)
            handle->request->allow_incidental_read.store(true, std::memory_order_relaxed);

    for (RequestHandle * handle : requests_to_start)
    {
        if (!*handle)
            continue;
        RequestState * req = handle->request;
        chassert(req);
        const auto s = req->state.load(std::memory_order_acquire);
        const Task * task = nullptr;
        if (s == RequestState::State::HasRange)
            task = pickRangesAndCreateTask(req, *handle, /*splitting*/ false, 0, 0);
        if (!task)
        {
            chassert(req->state.load(std::memory_order_relaxed) == RequestState::State::HasTask);
            task = req->range_or_task.task;
        }

        size_t memory_usage = size_t(req->length * task->memory_amplification);
        handle->memory = MemoryUsageToken(memory_usage, diff);
    }
}

Prefetcher::Task * Prefetcher::pickRangesAndCreateTask(RequestState * initial_req, const RequestHandle &, bool splitting, size_t subrange_start, size_t subrange_end)
{
    std::unique_lock lock(mutex);

    /// Re-check state after locking mutex.
    switch (initial_req->state.load(std::memory_order_acquire))
    {
        case RequestState::State::Cancelled: // impossible, we hold a RequestHandle
            chassert(false);
            break;
        case RequestState::State::HasRange:
            break;
        case RequestState::State::HasTask:
            /// Another thread created a task while we were locking the mutex.
            return nullptr;
    }
    size_t range_idx = initial_req->range_or_task.range_idx;
    chassert(range_idx < ranges.size());
    chassert(ranges[range_idx].request == initial_req);
    if (!splitting)
    {
        subrange_start = ranges[range_idx].start;
        subrange_end = ranges[range_idx].end;
    }

    /// Try to extend the task's range in both directions to cover more request ranges, as long
    /// as gaps between them are shorter than min_bytes_for_seek.

    size_t start_idx = range_idx;
    size_t end_idx = range_idx + 1;
    size_t total_length_of_covered_ranges = subrange_end - subrange_start;

    size_t initial_offset = subrange_start;
    size_t prev_offset = initial_offset;
    for (size_t idx = range_idx; idx > 0; --idx)
    {
        const RangeState & r = ranges[idx - 1];
        if (r.end + min_bytes_for_seek <= prev_offset || // short gap
            r.start + bytes_per_read_task <= initial_offset || // task not too big
            !r.request->allow_incidental_read.load(std::memory_order_relaxed)) // range wants to be coalesced
            break;

        const auto s = r.request->state.load(std::memory_order_relaxed);
        if (s == RequestState::State::HasRange)
        {
            /// Include this range in the task.
            start_idx = idx - 1;
            total_length_of_covered_ranges += r.length();
        }
        else if (s != RequestState::State::Cancelled)
        {
            /// Range already has a task. No need to scan further, the other task already did that.
            chassert(s == RequestState::State::HasTask);
            break;
        }
        else
        {
            /// Keep going past a cancelled range, but don't update start_idx until we see a
            /// non-cancelled range.
        }
        prev_offset = r.start;
    }
    initial_offset = ranges[start_idx].start;
    prev_offset = subrange_end;
    for (size_t idx = range_idx + 1; idx < ranges.size(); ++idx)
    {
        const RangeState & r = ranges[end_idx];
        if (prev_offset + min_bytes_for_seek <= r.start ||
            initial_offset + bytes_per_read_task <= r.end ||
            !r.request->allow_incidental_read.load(std::memory_order_relaxed))
            break;

        const auto s = r.request->state.load(std::memory_order_relaxed);
        if (s == RequestState::State::HasRange)
        {
            end_idx = idx + 1;
            total_length_of_covered_ranges += r.length();
        }
        else if (s != RequestState::State::Cancelled)
        {
            chassert(s == RequestState::State::HasTask);
            break;
        }

        prev_offset = r.end;
    }

    /// Create task.
    Task & task = tasks.emplace_back();
    task.offset = start_idx == range_idx ? subrange_start : ranges[start_idx].start;
    size_t end_offset = end_idx == range_idx + 1 ? subrange_end : ranges[end_idx - 1].end;
    task.length = end_offset - task.offset;
    task.memory_amplification = 1. * task.length / total_length_of_covered_ranges;
    size_t initial_refcount = end_idx - start_idx + 1;
    task.refcount.store(initial_refcount);

    size_t actual_refcount = splitting ? 1 : 0;
    for (size_t idx = start_idx; idx < end_idx; ++idx)
    {
        const RangeState & range = ranges[idx];
        RequestState * req = range.request;

        if (!splitting || idx != range_idx)
        {
            req->range_or_task.task = &task;
            req->task_offset = range.start - task.offset;

            RequestState::State s = RequestState::State::HasRange;
            if (req->state.compare_exchange_strong(s, RequestState::State::HasTask))
                actual_refcount += 1;
            else
                chassert(s == RequestState::State::Cancelled);
        }
    }

    decreaseTaskRefcount(&task, initial_refcount - actual_refcount);

    lock.unlock();
    scheduleTask(&task);

    return &task;
}

void Prefetcher::decreaseTaskRefcount(Task * task, size_t amount)
{
    size_t c = task->refcount.fetch_sub(amount, std::memory_order_acq_rel);
    chassert(c >= amount);
    if (c != amount)
        return;

    if (task->state.exchange(Task::State::Deallocated) != Task::State::Running)
        task->buf = {};
}

void Prefetcher::scheduleTask(Task * task)
{
    if (!thread_pool->io_runner.isDisabled())
        thread_pool->io_runner([this, task]
            {
                //TODO: thread group and thread name
                std::shared_lock shutdown_lock(*shutdown, std::try_to_lock);
                if (!shutdown_lock.owns_lock())
                    return;
                runTask(task);
            });
}

std::span<const char> Prefetcher::getRangeData(const RequestHandle & request)
{
    const RequestState * req = request.request;
    chassert(req->state == RequestState::State::HasTask);
    Task * task = req->range_or_task.task;
    auto s = task->state.load(std::memory_order_acquire);
    if (s == Task::State::Scheduled)
    {
        s = runTask(task);
        chassert(s != Task::State::Scheduled);
    }
    if (s == Task::State::Running)
    {
        task->completion.wait();
        s = task->state.load();
    }
    if (s == Task::State::Exception)
        rethrowException(task);
    chassert(s == Task::State::Done);
    chassert(req->task_offset + req->length <= task->buf.size());
    return std::span(task->buf.data() + req->task_offset, req->length);
}

Prefetcher::Task::State Prefetcher::runTask(Task * task)
{
    auto s = Task::State::Scheduled;
    if (!task->state.compare_exchange_strong(s, Task::State::Running))
        return s;
    auto final_state = Task::State::Done;
    try
    {
        task->buf.resize(task->length);
        readSync(task->buf.data(), task->length, task->offset);
    }
    catch (...)
    {
        final_state = Task::State::Exception;
        std::lock_guard lock(exception_mutex);
        task->exception = std::current_exception();
    }

    s = Task::State::Running;
    if (task->state.compare_exchange_strong(s, final_state))
    {
        s = final_state;
    }
    else
    {
        chassert(s == Task::State::Deallocated);
        task->buf = {};
    }

    task->completion.notify();

    return s;
}

void Prefetcher::splitAndPrefetchRange(
    RequestHandle request, const std::vector<std::pair</*global_offset*/ size_t, /*length*/ size_t>> & subranges, std::vector<RequestHandle> & out_handles, MemoryUsageDiff * diff)
{
    chassert(ranges_finalized.load(std::memory_order_relaxed));
    chassert(std::is_sorted(subranges.begin(), subranges.end()));
    chassert(!subranges.empty());
    RequestState * parent_req = request.request;
    chassert(parent_req->state.load(std::memory_order_relaxed) == RequestState::State::HasRange);
    size_t range_idx = parent_req->range_or_task.range_idx;
    const auto & range = ranges[range_idx];

    size_t subrange_start = UINT64_MAX;
    size_t subrange_end = 0;
    size_t subranges_total_length = 0;
    for (const auto & [start, length] : subranges)
    {
        if (start < range.start || length > range.end - start)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Subrange out of bounds: [{}, {}) not in [{}, {})", start, start + length, range.start, range.end);
        subrange_start = std::min(subrange_start, start);
        subrange_end = std::max(subrange_end, start + length);
        subranges_total_length += length;
    }
    out_handles.resize(subranges.size());

    /// If the request is already short, don't split it, and try to coalesce with other ranges.
    if (range.length() < min_bytes_for_seek)
    {
        Task * task = pickRangesAndCreateTask(parent_req, request, /*splitting*/ true, subrange_start, subrange_end);
        task->refcount.fetch_add(subranges.size() - 1);
        double memory_amplification = task->memory_amplification * (subrange_end - subrange_start) / subranges_total_length;

        std::lock_guard lock(mutex);

        for (size_t idx = 0; idx < subranges.size(); ++idx)
        {
            RequestState & req = requests.emplace_back();
            req.state.store(RequestState::State::HasTask, std::memory_order_relaxed);
            req.range_or_task.task = task;
            req.length = subranges[idx].second;
            req.task_offset = subranges[idx].first - task->offset;
            size_t memory_usage = size_t(req.length * memory_amplification);
            out_handles[idx] = RequestHandle(&req, MemoryUsageToken(memory_usage, diff));
        }
    }
    else
    {
        for (size_t start_idx = 0; start_idx < subranges.size(); )
        {
            /// Pick some consecutive subranges to read in one Task.
            size_t end_idx = start_idx + 1;
            size_t total_length = subranges[start_idx].second;
            while (end_idx < subranges.size() &&
                   // short gap
                   subranges[end_idx - 1].first + subranges[end_idx - 1].second + min_bytes_for_seek > subranges[end_idx].first &&
                   // task not too big
                   subranges[start_idx].first + bytes_per_read_task > subranges[end_idx].first + subranges[end_idx].second)
            {
                total_length += subranges[end_idx].second;
                end_idx += 1;
            }

            Task * task;
            {
                std::lock_guard lock(mutex);

                task = &tasks.emplace_back();
                task->offset = subranges[start_idx].first;
                task->length = subranges[end_idx - 1].first + subranges[end_idx - 1].second - task->offset;
                task->memory_amplification = 1. * task->length / total_length;
                task->refcount.store(end_idx - start_idx);

                for (size_t idx = start_idx; idx < end_idx; ++idx)
                {
                    RequestState & req = requests.emplace_back();
                    req.state.store(RequestState::State::HasTask, std::memory_order_relaxed);
                    req.range_or_task.task = task;
                    req.length = subranges[idx].second;
                    req.task_offset = subranges[idx].first - task->offset;
                    size_t memory_usage = size_t(task->memory_amplification * req.length);
                    out_handles[idx] = RequestHandle(&req, MemoryUsageToken(memory_usage, diff));
                }
            }

            scheduleTask(task);

            start_idx = end_idx;
        }
    }

    request.reset(diff);
}

void Prefetcher::rethrowException(Task * task)
{
    std::lock_guard lock(exception_mutex);
    if (task->exception)
        std::rethrow_exception(std::move(task->exception));
    else
        /// exception_ptr is not copyable, so we rethrow the correct exception only the first time,
        /// then throw uninformative exceptions if called again (e.g. for multiple requests covered
        /// by the same task). Hopefully the first thrown exception will usually be propagated to
        /// the user.
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read failed");
}

Prefetcher::RequestHandle::RequestHandle(RequestState * request_, MemoryUsageToken memory_)
    : request(request_), memory(std::move(memory_)) {}

Prefetcher::RequestHandle::RequestHandle(RequestHandle && rhs) noexcept
{
    *this = std::move(rhs);
}

Prefetcher::RequestHandle & Prefetcher::RequestHandle::operator=(RequestHandle && rhs) noexcept
{
    // Shouldn't assign to nonempty handles because deallocation wouldn't be recorded in MemoryUsageDiff.
    chassert(!memory);

    reset(nullptr);
    request = std::exchange(rhs.request, nullptr);
    return *this;
}

Prefetcher::RequestHandle::~RequestHandle()
{
    reset(nullptr);
}

void Prefetcher::RequestHandle::reset(MemoryUsageDiff * diff)
{
    if (!request)
        return;

    if (diff)
        memory.reset(diff);

    if (request->state.exchange(RequestState::State::Cancelled) == RequestState::State::HasTask)
        Prefetcher::decreaseTaskRefcount(request->range_or_task.task, 1);
}

}
