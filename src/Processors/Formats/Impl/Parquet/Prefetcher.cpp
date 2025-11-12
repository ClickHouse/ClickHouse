#include <Processors/Formats/Impl/Parquet/Prefetcher.h>

#include <Formats/FormatParserSharedResources.h>
#include <IO/copyData.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <IO/WriteBufferFromVector.h>

#include <shared_mutex>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace ProfileEvents
{
    extern const Event ParquetFetchWaitTimeMicroseconds;
}

namespace DB::Parquet
{

void Prefetcher::init(ReadBuffer * reader_, const ReadOptions & options, FormatParserSharedResourcesPtr parser_shared_resources_)
{
    min_bytes_for_seek = options.min_bytes_for_seek;
    bytes_per_read_task = options.bytes_per_read_task;
    parser_shared_resources = parser_shared_resources_;
    determineReadModeAndFileSize(reader_, options);
    range_sets.resize(1);
}

Prefetcher::~Prefetcher()
{
    shutdown->shutdown();

    /// Assert that all PrefetchHandle-s were destroyed.
    chassert(std::all_of(requests.begin(), requests.end(), [](const RequestState & req)
    {
        return req.state.load(std::memory_order_relaxed) == RequestState::State::Cancelled;
    }));
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

PrefetchHandle Prefetcher::registerRange(size_t offset, size_t length, bool likely_to_be_used)
{
    chassert(!ranges_finalized.load(std::memory_order_relaxed));
    if (offset > file_size || length > file_size - offset)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Range out of bounds: offset {}, length {}, file size {}", offset, length, file_size);
    RequestState & req = requests.emplace_back();
    req.length = length;
    req.allow_incidental_read.store(likely_to_be_used || length < min_bytes_for_seek, std::memory_order_relaxed);
    range_sets[0].ranges.push_back(RangeState {.request = &req, .start = offset, .end = offset + length});
    return PrefetchHandle(&req);
}

void Prefetcher::finalizeRanges()
{
    bool already_finalized = ranges_finalized.exchange(true);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(!already_finalized);
    auto & ranges = range_sets[0].ranges;
    std::sort(ranges.begin(), ranges.end(), [](const RangeState & a, const RangeState & b)
        {
            return a.start < b.start;
        });
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        RequestState * req = ranges[i].request;
        const auto s = req->state.load(std::memory_order_relaxed);
        if (s == RequestState::State::HasRange)
            req->range_idx = i;
        else
            chassert(s == RequestState::State::Cancelled);
    }
}

void Prefetcher::startPrefetch(const std::vector<PrefetchHandle *> & requests_to_start, MemoryUsageDiff * diff)
{
    chassert(ranges_finalized.load(std::memory_order_relaxed));

    /// Allow the requested ranges can be coalesced with each other even if they're longer than
    /// min_bytes_for_seek.
    for (const PrefetchHandle * handle : requests_to_start)
        if (*handle)
            handle->request->allow_incidental_read.store(true, std::memory_order_relaxed);

    for (PrefetchHandle * handle : requests_to_start)
    {
        if (!*handle)
            continue;
        RequestState * req = handle->request;
        chassert(req);
        pickRangesAndCreateTaskIfNotExists(req, *handle, /*splitting=*/ false, 0, 0, std::unique_lock(mutex));
        chassert(req->state.load(std::memory_order_relaxed) == RequestState::State::HasTask);
        const Task * task = req->task;

        if (!handle->memory)
        {
            size_t memory_usage = size_t(req->length * task->memory_amplification);
            handle->memory = MemoryUsageToken(memory_usage, diff);
        }
    }
}

std::vector<PrefetchHandle> Prefetcher::splitRange(
    PrefetchHandle request, const std::vector<std::pair</*global_offset*/ size_t, /*length*/ size_t>> & subranges, bool likely_to_be_used)
{
    chassert(ranges_finalized.load(std::memory_order_relaxed));
    chassert(std::is_sorted(subranges.begin(), subranges.end()));
    chassert(!subranges.empty());
    chassert(!request.memory); // prefetch not requested

    RequestState * parent_req = request.request;
    std::vector<PrefetchHandle> out_handles;

    {
        std::unique_lock lock(mutex);

        /// Allocate RequestState-s.
        out_handles.reserve(subranges.size());
        for (size_t i = 0; i < subranges.size(); ++i)
            out_handles.push_back(PrefetchHandle(&requests.emplace_back()));

        if (parent_req->state.load(std::memory_order_relaxed) == RequestState::State::HasRange)
        {
            auto & ranges = range_sets[parent_req->range_set_idx].ranges;
            const auto & range = ranges.at(parent_req->range_idx);

            size_t subrange_start = UINT64_MAX;
            size_t subrange_end = 0;
            for (const auto & [start, length] : subranges)
            {
                if (start < range.start || length > range.end - start)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Subrange out of bounds: [{}, {}) not in [{}, {})", start, start + length, range.start, range.end);
                subrange_start = std::min(subrange_start, start);
                subrange_end = std::max(subrange_end, start + length);
            }

            /// If the request is already short, don't split it, and try to coalesce with other ranges.
            if (range.length() < min_bytes_for_seek)
            {
                pickRangesAndCreateTaskIfNotExists(parent_req, request, /*splitting=*/ true, subrange_start, subrange_end, std::move(lock));
            }
            else
            {
                /// Normal case: actually split the range.
                ///
                /// We put the split ranges into a new universe instead of inserting into the middle
                /// of the existing RangeSet. This allows us to use a sorted array instead of a slow
                /// tree (e.g. std::map), but introduces a limitation: ranges produced by a split can only
                /// be coalesced among each other, not with other ranges (non-split ranges or ranges from
                /// other splits). (I just guessed that this would be a better tradeoff, didn't benchmark it.)
                size_t new_range_set_idx = range_sets.size();
                auto & new_ranges = range_sets.emplace_back().ranges;
                new_ranges.reserve(subranges.size());
                for (size_t i = 0; i < subranges.size(); ++i)
                {
                    const auto [start, length] = subranges[i];
                    RequestState * req = out_handles[i].request;
                    req->state.store(RequestState::State::HasRange, std::memory_order_relaxed);
                    req->allow_incidental_read.store(likely_to_be_used || length < min_bytes_for_seek);
                    req->range_set_idx = new_range_set_idx;
                    req->range_idx = i;
                    req->length = length;

                    RangeState & r = new_ranges.emplace_back();
                    r.start = start;
                    r.end = start + length;
                    r.request = req;
                }

                request.reset(/*diff=*/ nullptr);
                return out_handles;
            }
        }
    } // unlock mutex

    chassert(parent_req->state.load(std::memory_order_relaxed) == RequestState::State::HasTask);
    Task * task = parent_req->task;
    task->refcount.fetch_add(subranges.size());

    for (size_t i = 0; i < subranges.size(); ++i)
    {
        RequestState * req = out_handles[i].request;
        req->state.store(RequestState::State::HasTask, std::memory_order_relaxed);
        req->task = task;
        req->length = subranges[i].second;
        req->task_offset = subranges[i].first - task->offset;
    }

    request.reset(/*diff=*/ nullptr);
    return out_handles;
}

void Prefetcher::pickRangesAndCreateTaskIfNotExists(RequestState * initial_req, const PrefetchHandle &, bool splitting, size_t start_offset, size_t end_offset, std::unique_lock<std::mutex> lock)
{
    chassert(lock.owns_lock());

    /// Re-check state after locking mutex.
    switch (initial_req->state.load(std::memory_order_acquire))
    {
        case RequestState::State::Cancelled: // impossible, we hold a PrefetchHandle
            chassert(false);
            break;
        case RequestState::State::HasRange:
            break;
        case RequestState::State::HasTask:
            /// Another thread created a task while we were locking the mutex.
            return;
    }
    size_t range_set_idx = initial_req->range_set_idx;
    size_t range_idx = initial_req->range_idx;
    auto & ranges = range_sets.at(range_set_idx).ranges;
    chassert(ranges.at(range_idx).request == initial_req);
    if (!splitting)
    {
        start_offset = ranges[range_idx].start;
        end_offset = ranges[range_idx].end;
    }

    /// Try to extend the task's range in both directions to cover more request ranges, as long
    /// as gaps between them are shorter than min_bytes_for_seek.

    size_t start_idx = range_idx;
    size_t end_idx = range_idx + 1;
    size_t total_length_of_covered_ranges = end_offset - start_offset;

    /// Go left.
    size_t initial_offset = start_offset;
    for (size_t idx = range_idx; idx > 0; --idx)
    {
        const RangeState & r = ranges[idx - 1];
        if (r.end + min_bytes_for_seek <= start_offset || // short gap
            r.start + bytes_per_read_task <= initial_offset || // task not too big
            !r.request->allow_incidental_read.load(std::memory_order_relaxed)) // range wants to be coalesced
            break;

        const auto s = r.request->state.load(std::memory_order_relaxed);
        if (s == RequestState::State::HasRange)
        {
            /// Include this range in the task.
            start_idx = idx - 1;
            total_length_of_covered_ranges += r.length();
            start_offset = std::min(start_offset, r.start);
        }
        else if (s != RequestState::State::Cancelled)
        {
            /// Range already has a task. No need to scan further, the other task already did that.
            chassert(s == RequestState::State::HasTask);
            break;
        }
        else
        {
            /// Keep going past a cancelled range, but don't update start_idx/start_offset until we
            /// hit a non-cancelled range.
        }
    }

    /// Go right.
    initial_offset = end_offset;
    for (size_t idx = range_idx + 1; idx < ranges.size(); ++idx)
    {
        const RangeState & r = ranges[end_idx];
        if (end_offset + min_bytes_for_seek <= r.start ||
            initial_offset + bytes_per_read_task <= r.end ||
            !r.request->allow_incidental_read.load(std::memory_order_relaxed))
            break;

        const auto s = r.request->state.load(std::memory_order_relaxed);
        if (s == RequestState::State::HasRange)
        {
            end_idx = idx + 1;
            total_length_of_covered_ranges += r.length();
            end_offset = std::max(end_offset, r.end);
        }
        else if (s != RequestState::State::Cancelled)
        {
            chassert(s == RequestState::State::HasTask);
            break;
        }
    }

    /// Create task.
    Task & task = tasks.emplace_back();
    task.offset = start_offset;
    task.length = end_offset - task.offset;
    task.memory_amplification = 1. * task.length / total_length_of_covered_ranges;
    size_t initial_refcount = end_idx - start_idx + 1;
    task.refcount.store(initial_refcount);

    size_t actual_refcount = 0;
    for (size_t idx = start_idx; idx < end_idx; ++idx)
    {
        const RangeState & range = ranges[idx];
        RequestState * req = range.request;
        req->task = &task;
        req->task_offset = range.start - task.offset;

        RequestState::State s = RequestState::State::HasRange;
        if (req->state.compare_exchange_strong(s, RequestState::State::HasTask))
            actual_refcount += 1;
        else
            chassert(s == RequestState::State::Cancelled);
    }

    chassert(actual_refcount > 0);
    decreaseTaskRefcount(&task, initial_refcount - actual_refcount);

    lock.unlock();

    scheduleTask(&task);
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
    if (parser_shared_resources && !parser_shared_resources->io_runner.isDisabled())
        parser_shared_resources->io_runner([this, task, _shutdown = shutdown]
            {
                std::shared_lock shutdown_lock(*_shutdown, std::try_to_lock);
                if (!shutdown_lock.owns_lock())
                    return;
                runTask(task);
            });
}

std::span<const char> Prefetcher::getRangeData(const PrefetchHandle & request)
{
    const RequestState * req = request.request;
    chassert(req->state == RequestState::State::HasTask);
    Task * task = req->task;
    Task::State s = task->state.load(std::memory_order_acquire);
    if (s == Task::State::Scheduled || s == Task::State::Running)
    {
        Stopwatch wait_time;

        if (s == Task::State::Scheduled)
        {
            s = runTask(task);
            chassert(s != Task::State::Scheduled);
        }

        if (s == Task::State::Running) // (not `else`, the runTask above may return Running)
        {
            task->completion.wait();
            s = task->state.load();
        }

        ProfileEvents::increment(ProfileEvents::ParquetFetchWaitTimeMicroseconds, wait_time.elapsedMicroseconds());
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

void Prefetcher::rethrowException(Task * task)
{
    std::lock_guard lock(exception_mutex);
    if (task->exception)
        std::rethrow_exception(task->exception);
    else
        /// exception_ptr is not copyable, so we rethrow the correct exception only the first time,
        /// then throw uninformative exceptions if called again (e.g. for multiple requests covered
        /// by the same task). Hopefully the first thrown exception will usually be propagated to
        /// the user.
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read failed");
}

PrefetchHandle::PrefetchHandle(RequestState * request_) : request(request_) {}

PrefetchHandle::PrefetchHandle(PrefetchHandle && rhs) noexcept
{
    *this = std::move(rhs);
}

PrefetchHandle & PrefetchHandle::operator=(PrefetchHandle && rhs) noexcept
{
    // Shouldn't assign to nonempty handles because deallocation wouldn't be recorded in MemoryUsageDiff.
    chassert(!memory);

    reset(nullptr);
    request = std::exchange(rhs.request, nullptr);
    return *this;
}

PrefetchHandle::~PrefetchHandle()
{
    reset(nullptr);
}

void PrefetchHandle::reset(MemoryUsageDiff * diff)
{
    if (!request)
        return;

    if (diff)
        memory.reset(diff);

    if (request->state.exchange(RequestState::State::Cancelled) == RequestState::State::HasTask)
        Prefetcher::decreaseTaskRefcount(request->task, 1);

    request = nullptr;
}

}
