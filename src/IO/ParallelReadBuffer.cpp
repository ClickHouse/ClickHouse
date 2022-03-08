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

ParallelReadBuffer::ParallelReadBuffer(std::unique_ptr<ReadBufferFactory> reader_factory_, size_t max_working_readers)
    : SeekableReadBufferWithSize(nullptr, 0), pool(max_working_readers), reader_factory(std::move(reader_factory_))
{
    initializeWorkers();
}

void ParallelReadBuffer::initializeWorkers()
{
    for (size_t i = 0; i < pool.getMaxThreads(); ++i)
        pool.scheduleOrThrow([this] { processor(); });
}

off_t ParallelReadBuffer::seek(off_t offset, int whence)
{
    if (whence != SEEK_SET)
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset < 0)
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (!working_buffer.empty() && size_t(offset) >= current_position - working_buffer.size() && offset < current_position)
    {
        pos = working_buffer.end() - (current_position - offset);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return offset;
    }


    const auto offset_is_in_range
        = [&](const auto & range) { return static_cast<size_t>(offset) >= range.from && static_cast<size_t>(offset) < range.to; };

    std::unique_lock lock{mutex};
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
            next_condvar.wait(lock, [&] { return !segments.empty(); });

            if (static_cast<size_t>(offset) < current_position + segments.front().size())
            {
                segment = std::move(segments.front());
                segments.pop_front();
                working_buffer = internal_buffer = Buffer(segment.data(), segment.data() + segment.size());
                current_position += segment.size();
                front_worker->range.from += segment.size();
                pos = working_buffer.end() - (current_position - offset);
                return offset;
            }

            current_position += segments.front().size();
            front_worker->range.from += segments.front().size();
            segments.pop_front();
        }
    }

    lock.unlock();
    finishAndWait();

    reader_factory->seek(offset, whence);
    all_created = false;
    all_completed = false;
    read_workers.clear();

    current_position = offset;
    resetWorkingBuffer();

    emergency_stop = false;
    initializeWorkers();
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
                return emergency_stop || (all_created && read_workers.empty()) || currentWorkerReady();
            });

        if (emergency_stop)
        {
            if (background_exception)
                std::rethrow_exception(background_exception);
            else
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Emergency stop");
        }

        /// Remove completed units
        while (!read_workers.empty() && currentWorkerCompleted())
            read_workers.pop_front();

        /// All readers processed, stop
        if (read_workers.empty() && all_created)
        {
            all_completed = true;
            return false;
        }

        auto & front_worker = read_workers.front();
        /// Read data from first segment of the first reader
        if (!front_worker->segments.empty())
        {
            segment = std::move(front_worker->segments.front());
            front_worker->range.from += segment.size();
            front_worker->segments.pop_front();
            break;
        }
    }
    working_buffer = internal_buffer = Buffer(segment.data(), segment.data() + segment.size());
    current_position += working_buffer.size();
    return true;
}

void ParallelReadBuffer::processor()
{
    while (!emergency_stop)
    {
        ReadWorkerPtr worker;
        {
            /// Create new read worker and put in into end of queue
            /// reader_factory is not thread safe, so we call getReader under lock
            std::lock_guard lock(mutex);
            auto reader = reader_factory->getReader();
            if (!reader)
            {
                all_created = true;
                next_condvar.notify_all();
                break;
            }

            worker = read_workers.emplace_back(std::make_shared<ReadWorker>(std::move(reader->first), reader->second));
        }

        /// Start processing
        readerThreadFunction(std::move(worker));
    }
}

void ParallelReadBuffer::readerThreadFunction(ReadWorkerPtr read_worker)
{
    try
    {
        while (!emergency_stop)
        {
            if (!read_worker->reader->next())
            {
                std::lock_guard lock(mutex);
                read_worker->finished = true;
                next_condvar.notify_all();
                break;
            }

            if (emergency_stop)
                break;

            Buffer buffer = read_worker->reader->buffer();
            Memory<> new_segment(buffer.size());
            memcpy(new_segment.data(), buffer.begin(), buffer.size());
            {
                /// New data ready to be read
                std::lock_guard lock(mutex);
                read_worker->segments.emplace_back(std::move(new_segment));
                next_condvar.notify_all();
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
}

void ParallelReadBuffer::finishAndWait()
{
    emergency_stop = true;
    try
    {
        pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
