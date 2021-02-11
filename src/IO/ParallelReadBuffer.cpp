#include "ParallelReadBuffer.h"


namespace DB
{


namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

bool ParallelReadBuffer::nextImpl()
{
    if (all_done)
        return false;

    size_t unit_num = current_read_unit++ % processing_units.size();
    auto & unit = processing_units[unit_num];

    {
        std::unique_lock<std::mutex> lock(mutex);
        next_condvar.wait(lock, [this, &unit] {
            return emergency_stop || unit.status == ProcessingUnitStatus::READY_TO_READ || unit.status == ProcessingUnitStatus::STOPPED;
        });
    }
    if (emergency_stop)
    {
        if (background_exception)
            std::rethrow_exception(background_exception);
        else
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Emergency stop");
    }

    if (unit.status == ProcessingUnitStatus::STOPPED)
    {
        all_done = true;
        return false;
    }
    assert(unit.status == ProcessingUnitStatus::READY_TO_READ);

    /// Make buffer available to user
    segment.resize(unit.segment.size());
    memcpy(segment.data(), unit.segment.data(), unit.segment.size());
    working_buffer = internal_buffer = Buffer(segment.data(), segment.data() + segment.size());

    {
        std::unique_lock<std::mutex> lock(mutex);
        unit.status = ProcessingUnitStatus::READY_TO_INSERT;
        reader_condvar.notify_all();
    }

    return true;
}

void ParallelReadBuffer::readerUnitThreadFunction(size_t unit_number)
{
    try
    {
        auto & unit = processing_units[unit_number];
        while (!emergency_stop)
        {
            ReadBufferFactory::ReadBufferPtr reader = nullptr;
            {
                std::unique_lock<std::mutex> lock(mutex);
                reader_condvar.wait(lock, [this, &unit] { return emergency_stop || unit.status == ProcessingUnitStatus::READY_TO_INSERT; });
                if (emergency_stop)
                    return;

                reader = reader_factory->getReader();
                if (!reader)
                {
                    unit.status = ProcessingUnitStatus::STOPPED;
                    next_condvar.notify_all();
                    return;
                }
                unit.status = ProcessingUnitStatus::READING;
            }

            size_t read_at_once = unit.segment.size();
            size_t n = reader->read(unit.segment.data(), read_at_once);

            /// It's expected to have small readers
            while (unlikely(!emergency_stop && !reader->eof()))
            {
                unit.segment.resize(n + read_at_once);
                n += reader->read(unit.segment.data() + n, read_at_once);
            }
            if (emergency_stop)
                return;

            /// Trim suffix
            unit.segment.resize(n);
            /// New data can be read in nextImpl
            {
                std::lock_guard<std::mutex> lock(mutex);
                unit.status = ProcessingUnitStatus::READY_TO_READ;
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
