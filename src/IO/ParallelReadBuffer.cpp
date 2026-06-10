#include <IO/ParallelReadBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;

}

// A subrange of the input, read by one thread.
struct ParallelReadBuffer::ReadWorker
{
    ReadWorker(SeekableReadBuffer & input_, size_t offset, size_t size)
        : input(input_), start_offset(offset), segment(size)
    {
        chassert(size);
        chassert(segment.size() == size);
    }

    bool hasBytesToConsume() const { return bytes_produced > bytes_consumed; }
    bool hasBytesToProduce() const { return bytes_produced < segment.size(); }

    SeekableReadBuffer & input;
    const size_t start_offset; // start of the segment

    Memory<> segment;
    /// Reader thread produces data, nextImpl() consumes it.
    /// segment[bytes_consumed..bytes_produced-1] is data waiting to be picked up by nextImpl()
    /// segment[bytes_produced..] needs to be read from the input ReadBuffer
    size_t bytes_produced = 0;
    size_t bytes_consumed = 0;

    std::atomic_bool cancel{false};
    std::mutex worker_mutex;
};

ParallelReadBuffer::ParallelReadBuffer(
    SeekableReadBuffer & input_, ThreadPoolCallbackRunnerUnsafe<void> schedule_, size_t max_working_readers_, size_t range_step_, size_t file_size_)
    : SeekableReadBuffer(nullptr, 0)
    , max_working_readers(max_working_readers_)
    , schedule(std::move(schedule_))
    , input(input_)
    , file_size(file_size_)
    , range_step(std::max(1ul, range_step_))
{
    LOG_TRACE(getLogger("ParallelReadBuffer"), "Parallel reading is used");

    try
    {
        addReaders();
    }
    catch (const Exception &)
    {
        finishAndWait();
        throw;
    }
}

bool ParallelReadBuffer::addReaderToPool()
{
    if (next_range_start >= file_size)
        return false;
    size_t range_start = next_range_start;
    size_t size = std::min(range_step, file_size - range_start);
    next_range_start += size;

    auto worker = read_workers.emplace_back(std::make_shared<ReadWorker>(input, range_start, size));

    schedule([this, my_worker = std::move(worker)]() mutable { readerThreadFunction(std::move(my_worker)); }, Priority{});
    /// increase number of workers only after we are sure that the reader was scheduled
    ++active_working_readers;

    return true;
}

void ParallelReadBuffer::addReaders()
{
    while (read_workers.size() < max_working_readers && addReaderToPool())
        ;
}

off_t ParallelReadBuffer::seek(off_t offset, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    if (offset < 0)
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", offset);

    if (!working_buffer.empty() && static_cast<size_t>(offset) >= current_position - working_buffer.size() && offset < current_position)
    {
        pos = working_buffer.end() - (current_position - offset);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return offset;
    }

    const auto offset_is_in_range
        = [&](const auto & worker) { return static_cast<size_t>(offset) >= worker->start_offset && static_cast<size_t>(offset) < worker->start_offset + worker->segment.size(); };

    while (!read_workers.empty() && !offset_is_in_range(read_workers.front()))
    {
        read_workers.front()->cancel = true;
        read_workers.pop_front();
    }

    if (!read_workers.empty())
    {
        auto & w = read_workers.front();
        size_t diff = static_cast<size_t>(offset) - w->start_offset;
        while (true)
        {
            std::unique_lock lock{w->worker_mutex};

            if (emergency_stop)
                handleEmergencyStop();

            if (w->bytes_produced > diff)
            {
                working_buffer = internal_buffer = Buffer(
                    w->segment.data(), w->segment.data() + w->bytes_produced);
                pos = working_buffer.begin() + diff;
                w->bytes_consumed = w->bytes_produced;
                current_position = w->start_offset + w->bytes_consumed;
                addReaders();
                return offset;
            }

            next_condvar.wait_for(lock, std::chrono::seconds(10));
        }
    }

    finishAndWait();

    read_workers.clear();

    next_range_start = offset;
    current_position = offset;
    resetWorkingBuffer();

    emergency_stop = false;

    addReaders();
    return offset;
}

std::optional<size_t> ParallelReadBuffer::tryGetFileSize()
{
    return file_size;
}

off_t ParallelReadBuffer::getPosition()
{
    return current_position - available();
}

void ParallelReadBuffer::handleEmergencyStop()
{
    // this can only be called from the main thread when there is an exception
    assert(background_exception);
    std::rethrow_exception(background_exception);
}

bool ParallelReadBuffer::nextImpl()
{
    while (true)
    {
        /// All readers processed, stop
        if (read_workers.empty())
        {
            chassert(next_range_start >= file_size);
            return false;
        }

        auto * w = read_workers.front().get();

        std::unique_lock lock{w->worker_mutex};

        if (emergency_stop)
            handleEmergencyStop(); // throws

        /// Read data from front reader
        if (w->bytes_produced > w->bytes_consumed)
        {
            chassert(w->start_offset + w->bytes_consumed == static_cast<size_t>(current_position));

            working_buffer = internal_buffer = Buffer(
                w->segment.data() + w->bytes_consumed, w->segment.data() + w->bytes_produced);
            current_position += working_buffer.size();
            w->bytes_consumed = w->bytes_produced;

            return true;
        }

        /// Front reader is done, remove it and add another
        if (!w->hasBytesToProduce())
        {
            lock.unlock();
            read_workers.pop_front();
            addReaders();

            continue;
        }

        /// Nothing to do right now, wait for something to change.
        ///
        /// The timeout is a workaround for a race condition.
        /// emergency_stop is assigned while holding a *different* mutex from the one we're holding
        /// (exception_mutex vs worker_mutex). So it's possible that our emergency_stop check (above)
        /// happens before a onBackgroundException() call, but our wait(lock) happens after it.
        /// Then the wait may get stuck forever.
        ///
        /// Note that using wait(lock, [&]{ return emergency_stop || ...; }) wouldn't help because
        /// it does effectively the same "check, then wait" sequence.
        ///
        /// One possible proper fix would be to make onBackgroundException() lock all read_workers
        /// mutexes too (not necessarily simultaneously - just locking+unlocking them one by one
        /// between the emergency_stop change and the notify_all() would be enough), but then we
        /// need another mutex to protect read_workers itself...
        next_condvar.wait_for(lock, std::chrono::seconds(10));
    }
    chassert(false);
    return false;
}

void ParallelReadBuffer::readerThreadFunction(ReadWorkerPtr read_worker)
{
    SCOPE_EXIT({
        if (active_working_readers.fetch_sub(1) == 1)
            active_working_readers.notify_all();
    });

    try
    {
        auto on_progress = [&](size_t bytes_read) -> bool
        {
            if (emergency_stop || read_worker->cancel)
                return true;

            std::lock_guard lock(read_worker->worker_mutex);
            if (bytes_read <= read_worker->bytes_produced)
                return false;

            bool need_notify = read_worker->bytes_produced == read_worker->bytes_consumed;
            read_worker->bytes_produced = bytes_read;
            if (need_notify)
                next_condvar.notify_all();

            return false;
        };

        size_t r = input.readBigAt(read_worker->segment.data(), read_worker->segment.size(), read_worker->start_offset, on_progress);

        if (!on_progress(r) && r < read_worker->segment.size())
            throw Exception(
                ErrorCodes::UNEXPECTED_END_OF_FILE,
                "Failed to read all the data from the reader at offset {}, got {}/{} bytes",
                read_worker->start_offset, r, read_worker->segment.size());
    }
    catch (...)
    {
        onBackgroundException();
    }
}

void ParallelReadBuffer::onBackgroundException()
{
    std::lock_guard lock{exception_mutex};
    if (!background_exception)
        background_exception = std::current_exception();

    emergency_stop = true;
    next_condvar.notify_all();
}

void ParallelReadBuffer::finishAndWait()
{
    emergency_stop = true;

    size_t active_readers = active_working_readers.load();
    while (active_readers != 0)
    {
        active_working_readers.wait(active_readers);
        active_readers = active_working_readers.load();
    }
}

std::unique_ptr<ParallelReadBuffer> wrapInParallelReadBufferIfSupported(
    ReadBuffer & buf, ThreadPoolCallbackRunnerUnsafe<void> schedule, size_t max_working_readers,
    size_t range_step, size_t file_size)
{
    auto * seekable = dynamic_cast<SeekableReadBuffer*>(&buf);
    if (!seekable || !seekable->supportsReadAt())
        return nullptr;

    return std::make_unique<ParallelReadBuffer>(
        *seekable, schedule, max_working_readers, range_step, file_size);
}

}
