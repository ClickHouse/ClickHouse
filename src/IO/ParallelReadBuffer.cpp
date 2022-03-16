#include <IO/ParallelReadBuffer.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;

}

ParallelReadBuffer::ParallelReadBuffer(std::unique_ptr<ReadBufferFactory> reader_factory_, ThreadPool * pool_, size_t max_working_readers_)
    : SeekableReadBufferWithSize(nullptr, 0)
    , pool(pool_)
    , max_working_readers(max_working_readers_)
    , reader_factory(std::move(reader_factory_))
{
    std::unique_lock<std::mutex> lock{mutex};
    addReaders(lock);
}

bool ParallelReadBuffer::addReaderToPool(std::unique_lock<std::mutex> & /*buffer_lock*/)
{
    auto reader = reader_factory->getReader();
    if (!reader)
    {
        return false;
    }

    auto worker = read_workers.emplace_back(std::make_shared<ReadWorker>(std::move(reader->first), reader->second));

    ThreadGroupStatusPtr running_group = CurrentThread::isInitialized() && CurrentThread::get().getThreadGroup()
        ? CurrentThread::get().getThreadGroup()
        : MainThreadStatus::getInstance().getThreadGroup();

    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    pool->scheduleOrThrow(
        [&, this, running_group = std::move(running_group), query_context = std::move(query_context), worker = std::move(worker)]() mutable
        {
            ThreadStatus thread_status;

            /// Save query context if any, because cache implementation needs it.
            if (query_context)
                thread_status.attachQueryContext(query_context);

            /// To be able to pass ProfileEvents.
            if (running_group)
                thread_status.attachQuery(running_group);

            readerThreadFunction(std::move(worker));

            if (running_group)
                thread_status.detachQuery(false);
        });
    return true;
}

void ParallelReadBuffer::addReaders(std::unique_lock<std::mutex> & buffer_lock)
{
    while (read_workers.size() < max_working_readers && addReaderToPool(buffer_lock))
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

    std::unique_lock lock{mutex};
    const auto offset_is_in_range
        = [&](const auto & range) { return static_cast<size_t>(offset) >= range.from && static_cast<size_t>(offset) < range.to; };

    while (!read_workers.empty() && (offset < current_position || !offset_is_in_range(read_workers.front()->range)))
    {
        read_workers.pop_front();
    }

    if (!read_workers.empty())
    {
        auto & front_worker = read_workers.front();
        auto & segments = front_worker->segments;
        current_position = front_worker->range.from;
        while (true)
        {
            next_condvar.wait(lock, [&] { return emergency_stop || !segments.empty(); });

            if (emergency_stop)
                handleEmergencyStop();

            auto next_segment = front_worker->nextSegment();
            if (static_cast<size_t>(offset) < current_position + next_segment.size())
            {
                current_segment = std::move(next_segment);
                working_buffer = internal_buffer = Buffer(current_segment.data(), current_segment.data() + current_segment.size());
                current_position += current_segment.size();
                pos = working_buffer.end() - (current_position - offset);
                addReaders(lock);
                return offset;
            }

            current_position += next_segment.size();
        }
    }

    lock.unlock();
    finishAndWait();

    reader_factory->seek(offset, whence);
    all_completed = false;
    read_workers.clear();

    current_position = offset;
    resetWorkingBuffer();

    emergency_stop = false;

    lock.lock();
    addReaders(lock);
    return offset;
}

std::optional<size_t> ParallelReadBuffer::getTotalSize()
{
    std::lock_guard lock{mutex};
    return reader_factory->getTotalSize();
}

off_t ParallelReadBuffer::getPosition()
{
    return current_position - available();
}

bool ParallelReadBuffer::currentWorkerReady() const
{
    return !read_workers.empty() && (read_workers.front()->finished || !read_workers.front()->segments.empty());
}

bool ParallelReadBuffer::currentWorkerCompleted() const
{
    return !read_workers.empty() && read_workers.front()->finished && read_workers.front()->segments.empty();
}

void ParallelReadBuffer::handleEmergencyStop()
{
    if (background_exception)
        std::rethrow_exception(background_exception);
    else
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Emergency stop");
}

bool ParallelReadBuffer::nextImpl()
{
    if (all_completed)
        return false;

    while (true)
    {
        std::unique_lock lock(mutex);
        next_condvar.wait(
            lock,
            [this]()
            {
                /// Check if no more readers left or current reader can be processed
                return emergency_stop || currentWorkerReady();
            });

        if (emergency_stop)
            handleEmergencyStop();

        bool worker_removed = false;
        /// Remove completed units
        while (!read_workers.empty() && currentWorkerCompleted())
        {
            read_workers.pop_front();
            worker_removed = true;
        }

        if (worker_removed)
            addReaders(lock);

        /// All readers processed, stop
        if (read_workers.empty())
        {
            all_completed = true;
            return false;
        }

        auto & front_worker = read_workers.front();
        /// Read data from first segment of the first reader
        if (!front_worker->segments.empty())
        {
            current_segment = front_worker->nextSegment();
            if (currentWorkerCompleted())
            {
                read_workers.pop_front();
                all_completed = !addReaderToPool(lock) && read_workers.empty();
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
    {
        std::lock_guard lock{mutex};
        ++active_working_reader;
    }

    SCOPE_EXIT({
        std::lock_guard lock{mutex};
        --active_working_reader;
        if (active_working_reader == 0)
        {
            readers_done.notify_all();
        }
    });

    try
    {
        while (!emergency_stop)
        {
            if (!read_worker->reader->next())
                throw Exception("Failed to read all the data from the reader", ErrorCodes::LOGICAL_ERROR);

            if (emergency_stop)
                break;

            Buffer buffer = read_worker->reader->buffer();
            size_t bytes_to_copy = std::min(buffer.size(), read_worker->bytes_left);
            Segment new_segment(bytes_to_copy, &arena);
            memcpy(new_segment.data(), buffer.begin(), bytes_to_copy);
            read_worker->reader->ignore(bytes_to_copy);
            read_worker->bytes_left -= bytes_to_copy;
            {
                /// New data ready to be read
                std::lock_guard lock(mutex);
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
    std::lock_guard lock(mutex);
    if (!background_exception)
    {
        background_exception = std::current_exception();
    }
    emergency_stop = true;
    next_condvar.notify_all();
}

void ParallelReadBuffer::finishAndWait()
{
    emergency_stop = true;

    std::unique_lock lock{mutex};
    readers_done.wait(lock, [&] { return active_working_reader == 0; });
}

}
