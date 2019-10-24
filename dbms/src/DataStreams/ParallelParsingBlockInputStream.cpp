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

            if (original_buffer.eof())
            {
                is_last[current_unit_number] = true;
                status[current_unit_number] = READY_TO_PARSE;
                scheduleParserThreadForUnitWithNumber(current_unit_number);
                break;
            }

            // Segmentating the original input.
            segments[current_unit_number].used_size = 0;

            //It returns bool, but it is useless
            file_segmentation_engine(original_buffer, segments[current_unit_number].memory, segments[current_unit_number].used_size, min_chunk_size);

            // Creating buffer from the segment of data.
            auto new_buffer = BufferBase::Buffer(segments[current_unit_number].memory.data(),
                                                 segments[current_unit_number].memory.data() + segments[current_unit_number].used_size);

            buffers[current_unit_number]->buffer().swap(new_buffer);
            buffers[current_unit_number]->position() = buffers[current_unit_number]->buffer().begin();

            readers[current_unit_number] = std::make_unique<InputStreamFromInputFormat> (
                    input_processor_creator(*buffers[current_unit_number], header, context, row_input_format_params, format_settings)
            );

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

            blocks[current_unit_number].block.clear();
            blocks[current_unit_number].block_missing_values.clear();

            if (is_last[current_unit_number] || buffers[current_unit_number]->position() == nullptr)
            {
                blocks[current_unit_number].block.emplace_back(Block());
                blocks[current_unit_number].block_missing_values.emplace_back(BlockMissingValues());
                status[current_unit_number] = READY_TO_READ;
                reader_condvar.notify_all();
                return;
            }

        }

        //We don't know how many blocks will be. So we have to read them all until an empty block occured.
        while (true)
        {
            auto block = readers[current_unit_number]->read();

            if (block == Block())
                break;

            blocks[current_unit_number].block.emplace_back(block);
            blocks[current_unit_number].block_missing_values.emplace_back(readers[current_unit_number]->getMissingValues());
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
        tryLogCurrentException(__PRETTY_FUNCTION__);
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
    const auto current_number = reader_ticket_number % max_threads_to_use;
    reader_condvar.wait(lock, [&](){ return status[current_number] == READY_TO_READ || is_exception_occured || is_cancelled; });

    /// Check for an exception and rethrow it
    if (is_exception_occured)
    {
        segmentator_condvar.notify_all();
        lock.unlock();
        cancel(false);
        rethrowFirstException(exceptions);
    }

    res = std::move(blocks[current_number].block.at(internal_block_iter));
    last_block_missing_values = std::move(blocks[current_number].block_missing_values[internal_block_iter]);

    if (++internal_block_iter == blocks[current_number].block.size())
    {
        if (is_last[current_number])
            is_cancelled = true;
        internal_block_iter = 0;
        ++reader_ticket_number;
        status[current_number] = READY_TO_INSERT;
        segmentator_condvar.notify_all();
    }

    return res;
}
}
