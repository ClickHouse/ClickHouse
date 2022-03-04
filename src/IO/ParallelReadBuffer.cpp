#include <IO/ParallelReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ParallelReadBuffer::ParallelReadBuffer(std::unique_ptr<ReadBufferFactory> reader_factory_, size_t max_working_readers)
    : ReadBuffer(nullptr, 0)
    , pool(max_working_readers)
    , reader_factory(std::move(reader_factory_))
{
    /// Create required number of processors.
    /// Processor start working after constructor exits.
    std::lock_guard<std::mutex> lock(mutex);
    for (size_t i = 0; i < max_working_readers; ++i)
        pool.scheduleOrThrow([this] { processor(); });
}

bool ParallelReadBuffer::nextImpl()
{
    if (all_completed)
        return false;

    while (true)
    {
        std::unique_lock<std::mutex> lock(mutex);
        next_condvar.wait(lock, [this]()
        {
            /// Check if no more readers left or current reader can be processed
            return emergency_stop || read_workers.empty() || currentWorkerReady();
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
        if (read_workers.empty())
        {
            all_completed = true;
            return false;
        }

        /// Read data from first segment of the first reader
        if (!read_workers.front()->segments.empty())
        {
            segment = std::move(read_workers.front()->segments.front());
            read_workers.front()->segments.pop_front();
            break;
        }
    }
    working_buffer = internal_buffer = Buffer(segment.data(), segment.data() + segment.size());
    return true;
}

void ParallelReadBuffer::processor()
{
    while (true)
    {
        ReadWorkerPtr worker;
        {
            /// Create new read worker and put in into end of queue
            /// reader_factory is not thread safe, so we call getReader under lock
            std::lock_guard<std::mutex> lock(mutex);
            auto reader = reader_factory->getReader();
            if (reader == nullptr)
                break;

            worker = read_workers.emplace_back(std::make_shared<ReadWorker>(reader));
        }

        /// Start processing
        readerThreadFunction(worker);
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
                std::lock_guard<std::mutex> lock(mutex);
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
                std::lock_guard<std::mutex> lock(mutex);
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
    std::unique_lock<std::mutex> lock(mutex);
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
