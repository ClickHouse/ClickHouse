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

            {
                std::unique_lock lock(mutex);
                segmentator_condvar.wait(lock, [&]{ return status[current_unit_number] == READY_TO_INSERT || is_exception_occured || is_cancelled; });
            }

            if (is_exception_occured)
                break;

            // Segmentating the original input.
            segments[current_unit_number].used_size = 0;
            bool has_data = file_segmentation_engine(original_buffer, segments[current_unit_number].memory, segments[current_unit_number].used_size, min_chunk_size);

            // Creating buffer from the segment of data.
            auto new_buffer = BufferBase::Buffer(segments[current_unit_number].memory.data(), segments[current_unit_number].memory.data() + segments[current_unit_number].used_size);
            buffers[current_unit_number]->buffer().swap(new_buffer);
            buffers[current_unit_number]->position() = buffers[current_unit_number]->buffer().begin();

            if (!has_data)
            {
                is_last[current_unit_number] = true;
                status[current_unit_number] = READY_TO_PARSE;
                scheduleParserThreadForUnitWithNumber(current_unit_number);
                break;
            }

            status[current_unit_number] = READY_TO_PARSE;
            scheduleParserThreadForUnitWithNumber(current_unit_number);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ParallelParsingBlockInputStream::parserThreadFunction(size_t current_unit_number)
{
    setThreadName("ChunkParser");

    if (is_exception_occured && is_cancelled)
        return;

    try
    {
        {
            std::unique_lock lock(mutex);

            if (is_last[current_unit_number] || buffers[current_unit_number]->position() == nullptr)
            {
                blocks[current_unit_number].block = Block();
                status[current_unit_number] = READY_TO_READ;
                reader_condvar.notify_all();
                return;
            }

        }

        blocks[current_unit_number].block = readers[current_unit_number]->read();

        {
            std::lock_guard missing_values_lock(missing_values_mutex);
            blocks[current_unit_number].block_missing_values = readers[current_unit_number]->getMissingValues();
        }

        {
            std::unique_lock lock(mutex);
            status[current_unit_number] = READY_TO_READ;
            reader_condvar.notify_all();
        }

    }
    catch (...)
    {
        std::unique_lock lock(mutex);
        exceptions[current_unit_number] = std::current_exception();
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

    reader_condvar.wait(lock, [&](){ return status[unit_number] == READY_TO_READ || is_exception_occured || is_cancelled; });

    /// Check for an exception and rethrow it
    if (is_exception_occured)
    {
        segmentator_condvar.notify_all();
        lock.unlock();
        cancel(false);
        rethrowFirstException(exceptions);
    }

    res = std::move(blocks[unit_number].block);
    last_block_missing_values = std::move(blocks[unit_number].block_missing_values);

    if (is_last[unit_number])
        is_cancelled = true;
    else
    {
        status[unit_number] = READY_TO_INSERT;
        segmentator_condvar.notify_all();
    }
    return res;
}
}
