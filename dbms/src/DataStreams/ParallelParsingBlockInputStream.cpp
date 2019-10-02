#include <DataStreams/ParallelParsingBlockInputStream.h>

namespace DB
{

void ParallelParsingBlockInputStream::segmentatorThreadFunction()
{
    setThreadName("Segmentator");
    try
    {
        while (!is_cancelled && !is_exception_occured)
        {
            ++segmentator_ticket_number;
            const auto current_unit_number = segmentator_ticket_number % max_threads_to_use;
            auto & current_unit = working_field[current_unit_number];

            {
                std::unique_lock lock(mutex);
                segmentator_condvar.wait(lock, [&](){ return current_unit.status == READY_TO_INSERT || is_exception_occured || is_cancelled; });
            }

            if (is_exception_occured)
                break;

            // Segmentating the original input.
            current_unit.chunk.used_size = 0;
            bool has_data = file_segmentation_engine(original_buffer, current_unit.chunk.memory, current_unit.chunk.used_size, min_chunk_size);

            // Creating buffer from the segment of data.
            auto new_buffer = BufferBase::Buffer(current_unit.chunk.memory.data(), current_unit.chunk.memory.data() + current_unit.chunk.used_size);
            current_unit.readbuffer.buffer().swap(new_buffer);
            current_unit.readbuffer.position() = current_unit.readbuffer.buffer().begin();

            if (!has_data)
            {
                std::unique_lock lock(mutex);
                current_unit.is_last_unit = true;
                current_unit.status = READY_TO_PARSE;
                scheduleParserThreadForUnitWithNumber(current_unit_number);
                break;
            }

            std::unique_lock lock(mutex);
            current_unit.status = READY_TO_PARSE;
            scheduleParserThreadForUnitWithNumber(current_unit_number);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ParallelParsingBlockInputStream::parserThreadFunction(size_t bucket_num)
{
    setThreadName("ChunkParser");

    if (is_exception_occured && is_cancelled)
        return;

    auto & current_unit = working_field[bucket_num];

    try
    {
        {
            std::unique_lock lock(mutex);
            if (current_unit.is_last_unit || current_unit.readbuffer.position() == nullptr)
            {
                current_unit.block = Block();
                current_unit.status = READY_TO_READ;
                reader_condvar.notify_all();
                return;
            }

        }

        current_unit.block = current_unit.reader->read();

        {
            std::lock_guard missing_values_lock(missing_values_mutex);
            block_missing_values = current_unit.reader->getMissingValues();
        }

        {
            std::unique_lock lock(mutex);
            current_unit.status = READY_TO_READ;
            reader_condvar.notify_all();
        }

    }
    catch (...)
    {
        std::unique_lock lock(mutex);
        exceptions[bucket_num] = std::current_exception();
        is_exception_occured = true;
        reader_condvar.notify_all();
    }
}


Block ParallelParsingBlockInputStream::readImpl()
{
    Block res;
    if (isCancelledOrThrowIfKilled())
        return res;

    std::unique_lock lock(mutex);

    ++reader_ticket_number;
    const auto unit_number = reader_ticket_number % max_threads_to_use;
    auto & current_processed_unit = working_field[unit_number];

    reader_condvar.wait(lock, [&](){ return current_processed_unit.status == READY_TO_READ || is_exception_occured || is_cancelled; });

    /// Check for an exception and rethrow it
    if (is_exception_occured)
    {
        segmentator_condvar.notify_all();
        lock.unlock();
        cancel(false);
        rethrowFirstException(exceptions);
    }

    res = std::move(current_processed_unit.block);

    if (current_processed_unit.is_last_unit)
        is_cancelled = true;
    else
    {
        current_processed_unit.status = READY_TO_INSERT;
        segmentator_condvar.notify_all();
    }
    return res;
}

}
