#include "ReadBufferFanIn.h"


namespace DB
{


namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

bool ReadBufferFanIn::nextImpl()
{
    if (all_done)
        return false;

    while (true)
    {
        std::unique_lock<std::mutex> lock(mutex);
        next_condvar.wait(
            lock, [this] { return emergency_stop || readers.empty() || readers.front()->finished || !readers.front()->segments.empty(); });

        if (emergency_stop)
        {
            if (background_exception)
                std::rethrow_exception(background_exception);
            else
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Emergency stop");
        }

        /// Remove completed units
        while (!readers.empty() && readers.front()->finished && readers.front()->segments.empty())
            readers.pop_front();

        if (readers.empty())
        {
            all_done = true;
            return false;
        }

        /// Read data from first segment of the first reader
        if (!readers.front()->segments.empty())
        {
            segment = std::move(readers.front()->segments.front());
            readers.front()->segments.pop_front();
            reader_condvar.notify_all();
            break;
        }
        reader_condvar.notify_all();
    }
    working_buffer = internal_buffer = Buffer(segment.data(), segment.data() + segment.size());
    return true;
}

void ReadBufferFanIn::addReader(ReadBufferPtr reader)
{
    if (started)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Calling addReader after start");

    readers.emplace_back(std::make_shared<ProcessingUnit>(reader, last_num++));
}

void ReadBufferFanIn::start()
{
    started = true;

    for (size_t i = 0; i < max_working_readers; ++i)
        pool.scheduleOrThrow([this]
        {
            while (auto reader = chooseNextReader())
                readerThreadFunction(reader);
        });
}

ReadBufferFanIn::ProcessingUnitPtr ReadBufferFanIn::chooseNextReader()
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & reader : readers)
    {
        if (reader->executing)
            continue;
        reader->executing = true;
        return reader;
    }
    return nullptr;
}

void ReadBufferFanIn::readerThreadFunction(ProcessingUnitPtr unit)
{
    try
    {
        while (!emergency_stop)
        {
            if (!unit->reader->next())
            {
                std::lock_guard<std::mutex> lock(mutex);
                unit->finished = true;
                next_condvar.notify_all();
                break;
            }

            {
                std::unique_lock<std::mutex> lock(mutex);
                reader_condvar.wait(lock, [this, unit]
                {
                    return emergency_stop || max_segments_per_worker == 0 || unit->segments.size() < max_segments_per_worker;
                });
            }

            if (emergency_stop)
                break;

            Buffer buffer = unit->reader->buffer();
            Memory<> new_segment(buffer.size());
            memcpy(new_segment.data(), buffer.begin(), buffer.size());
            {

                /// New data ready to be read
                std::lock_guard<std::mutex> lock(mutex);
                unit->segments.emplace_back(std::move(new_segment));
                next_condvar.notify_all();
            }
        }
    }
    catch (...)
    {
        onBackgroundException();
    }
}

void ReadBufferFanIn::onBackgroundException()
{
    std::unique_lock<std::mutex> lock(mutex);
    if (!background_exception)
    {
        background_exception = std::current_exception();
    }
    emergency_stop = true;
}

void ReadBufferFanIn::finishAndWait()
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
