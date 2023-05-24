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

struct ParallelReadBuffer::ReadWorker
{
    explicit ReadWorker(SeekableReadBufferPtr reader_) : reader(std::move(reader_)), range(reader->getRemainingReadRange())
    {
        assert(range.right);
        bytes_left = *range.right - range.left + 1;
    }

    auto hasSegment() const { return current_segment_index < segments.size(); }

    auto nextSegment()
    {
        assert(hasSegment());
        auto next_segment = std::move(segments[current_segment_index]);
        ++current_segment_index;
        range.left += next_segment.size();
        return next_segment;
    }

    SeekableReadBufferPtr reader;
    std::vector<Memory<>> segments;
    size_t current_segment_index = 0;
    bool finished{false};
    SeekableReadBuffer::Range range;
    size_t bytes_left{0};
    std::atomic_bool cancel{false};
    std::mutex worker_mutex;
};

ParallelReadBuffer::ParallelReadBuffer(
    std::unique_ptr<ReadBufferFactory> reader_factory_, CallbackRunner schedule_, size_t max_working_readers_)
    : SeekableReadBuffer(nullptr, 0)
    , max_working_readers(max_working_readers_)
    , schedule(std::move(schedule_))
    , reader_factory(std::move(reader_factory_))
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
    auto reader = reader_factory->getReader();
    if (!reader)
    {
        return false;
    }

    auto worker = read_workers.emplace_back(std::make_shared<ReadWorker>(std::move(reader)));

    ++active_working_reader;
    schedule([this, worker = std::move(worker)]() mutable { readerThreadFunction(std::move(worker)); });

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
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (!working_buffer.empty() && static_cast<size_t>(offset) >= current_position - working_buffer.size() && offset < current_position)
    {
        pos = working_buffer.end() - (current_position - offset);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return offset;
    }

    const auto offset_is_in_range
        = [&](const auto & range) { return static_cast<size_t>(offset) >= range.left && static_cast<size_t>(offset) <= *range.right; };

    while (!read_workers.empty() && (offset < current_position || !offset_is_in_range(read_workers.front()->range)))
    {
        read_workers.front()->cancel = true;
        read_workers.pop_front();
    }

    if (!read_workers.empty())
    {
        auto & front_worker = read_workers.front();
        current_position = front_worker->range.left;
        while (true)
        {
            std::unique_lock lock{front_worker->worker_mutex};
            next_condvar.wait(lock, [&] { return emergency_stop || front_worker->hasSegment(); });

            if (emergency_stop)
                handleEmergencyStop();

            auto next_segment = front_worker->nextSegment();
            if (static_cast<size_t>(offset) < current_position + next_segment.size())
            {
                current_segment = std::move(next_segment);
                working_buffer = internal_buffer = Buffer(current_segment.data(), current_segment.data() + current_segment.size());
                current_position += current_segment.size();
                pos = working_buffer.end() - (current_position - offset);
                addReaders();
                return offset;
            }

            current_position += next_segment.size();
        }
    }

    finishAndWait();

    reader_factory->seek(offset, whence);
    all_completed = false;
    read_workers.clear();

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
