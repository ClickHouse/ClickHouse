#include <IO/ParallelReadBuffer.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;

}

// A subrange of the input, read by one SeekableReadBuffer.
struct ParallelReadBuffer::ReadWorker
{
    ReadWorker(std::unique_ptr<SeekableReadBuffer> reader_, size_t offset_, size_t size)
        : reader(std::move(reader_)), offset(offset_), bytes_left(size), range_end(offset + bytes_left)
    {
        assert(bytes_left);
    }

    auto hasSegment() const { return current_segment_index < segments.size(); }

    auto nextSegment()
    {
        assert(hasSegment());
        auto next_segment = std::move(segments[current_segment_index]);
        ++current_segment_index;
        offset += next_segment.size();
        return next_segment;
    }

    std::unique_ptr<SeekableReadBuffer> reader;
    // Reader thread produces segments, nextImpl() consumes them.
    std::vector<Memory<>> segments; // segments that were produced
    size_t current_segment_index = 0; // first segment that's not consumed
    bool finished{false}; // no more segments will be produced
    size_t offset; // start of segments[current_segment_idx]
    size_t bytes_left; // bytes left to produce above segments end
    size_t range_end; // segments end + bytes_left, i.e. how far this worker will read

    //                  segments[current_segment_idx..end]           range_end
    // |-------------|--------------------------------------|------------|
    //             offset                                     bytes_left

    std::atomic_bool cancel{false};
    std::mutex worker_mutex;
};

ParallelReadBuffer::ParallelReadBuffer(
    std::unique_ptr<SeekableReadBufferFactory> reader_factory_, ThreadPoolCallbackRunner<void> schedule_, size_t max_working_readers_, size_t range_step_)
    : SeekableReadBuffer(nullptr, 0)
    , max_working_readers(max_working_readers_)
    , schedule(std::move(schedule_))
    , reader_factory(std::move(reader_factory_))
    , range_step(std::max(1ul, range_step_))
{
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
    size_t file_size = reader_factory->getFileSize();
    if (next_range_start >= file_size)
        return false;
    size_t range_start = next_range_start;
    size_t size = std::min(range_step, file_size - range_start);
    next_range_start += size;

    auto reader = reader_factory->getReader();
    if (!reader)
    {
        return false;
    }

    auto worker = read_workers.emplace_back(std::make_shared<ReadWorker>(std::move(reader), range_start, size));

    ++active_working_reader;
    schedule([this, worker = std::move(worker)]() mutable { readerThreadFunction(std::move(worker)); }, 0);

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
        = [&](const auto & worker) { return static_cast<size_t>(offset) >= worker->offset && static_cast<size_t>(offset) < worker->range_end; };

    while (!read_workers.empty() && (offset < current_position || !offset_is_in_range(read_workers.front())))
    {
        read_workers.front()->cancel = true;
        read_workers.pop_front();
    }

    if (!read_workers.empty())
    {
        auto & front_worker = read_workers.front();
        current_position = front_worker->offset;
        while (true)
        {
            std::unique_lock lock{front_worker->worker_mutex};
            next_condvar.wait(lock, [&] { return emergency_stop || front_worker->hasSegment(); });

            if (emergency_stop)
                handleEmergencyStop();

            auto next_segment = front_worker->nextSegment();
            current_position += next_segment.size();
            if (offset < current_position)
            {
                current_segment = std::move(next_segment);
                working_buffer = internal_buffer = Buffer(current_segment.data(), current_segment.data() + current_segment.size());
                pos = working_buffer.end() - (current_position - offset);
                addReaders();
                return offset;
            }
        }
    }

    finishAndWait();

    all_completed = false;
    read_workers.clear();

    next_range_start = offset;
    current_position = offset;
    resetWorkingBuffer();

    emergency_stop = false;

    addReaders();
    return offset;
}

size_t ParallelReadBuffer::getFileSize()
{
    return reader_factory->getFileSize();
}

off_t ParallelReadBuffer::getPosition()
{
    return current_position - available();
}

bool ParallelReadBuffer::currentWorkerReady() const
{
    assert(!read_workers.empty());
    return read_workers.front()->finished || read_workers.front()->hasSegment();
}

bool ParallelReadBuffer::currentWorkerCompleted() const
{
    return read_workers.front()->finished && !read_workers.front()->hasSegment();
}

void ParallelReadBuffer::handleEmergencyStop()
{
    // this can only be called from the main thread when there is an exception
    assert(background_exception);
    std::rethrow_exception(background_exception);
}

bool ParallelReadBuffer::nextImpl()
{
    if (all_completed)
        return false;

    while (true)
    {
        std::unique_lock lock{read_workers.front()->worker_mutex};
        next_condvar.wait(
            lock,
            [this]()
            {
                /// Check if no more readers left or current reader can be processed
                return emergency_stop || currentWorkerReady();
            });

        bool worker_removed = false;
        /// Remove completed units
        while (currentWorkerCompleted() && !emergency_stop)
        {
            lock.unlock();
            read_workers.pop_front();
            worker_removed = true;

            if (read_workers.empty())
                break;

            lock = std::unique_lock{read_workers.front()->worker_mutex};
        }

        if (emergency_stop)
            handleEmergencyStop();

        if (worker_removed)
            addReaders();

        /// All readers processed, stop
        if (read_workers.empty())
        {
            all_completed = true;
            return false;
        }

        auto & front_worker = read_workers.front();
        /// Read data from first segment of the first reader
        if (front_worker->hasSegment())
        {
            current_segment = front_worker->nextSegment();
            if (currentWorkerCompleted())
            {
                lock.unlock();
                read_workers.pop_front();
                all_completed = !addReaderToPool() && read_workers.empty();
            }
            break;
        }
    }
    working_buffer = internal_buffer = Buffer(current_segment.data(), current_segment.data() + current_segment.size());
    current_position += working_buffer.size();
    return true;
}

void ParallelReadBuffer::readerThreadFunction(ReadWorkerPtr read_worker)
{
    SCOPE_EXIT({
        if (active_working_reader.fetch_sub(1) == 1)
            active_working_reader.notify_all();
    });

    try
    {
        read_worker->reader->setReadUntilPosition(read_worker->range_end);
        read_worker->reader->seek(read_worker->offset, SEEK_SET);

        while (!emergency_stop && !read_worker->cancel)
        {
            if (!read_worker->reader->next())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Failed to read all the data from the reader, missing {} bytes", read_worker->bytes_left);

            if (emergency_stop || read_worker->cancel)
                break;

            Buffer buffer = read_worker->reader->buffer();
            size_t bytes_to_copy = std::min(buffer.size(), read_worker->bytes_left);
            Memory<> new_segment(bytes_to_copy);
            memcpy(new_segment.data(), buffer.begin(), bytes_to_copy);
            read_worker->reader->ignore(bytes_to_copy);
            read_worker->bytes_left -= bytes_to_copy;
            {
                /// New data ready to be read
                std::lock_guard lock(read_worker->worker_mutex);
                read_worker->segments.emplace_back(std::move(new_segment));
                read_worker->finished = read_worker->bytes_left == 0;
                next_condvar.notify_all();
            }

            if (read_worker->finished)
            {
                break;
            }
        }
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

    size_t active_readers = active_working_reader.load();
    while (active_readers != 0)
    {
        active_working_reader.wait(active_readers);
        active_readers = active_working_reader.load();
    }
}

}
