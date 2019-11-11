#include <DataStreams/ParallelParsingBlockInputStream.h>
#include "ParallelParsingBlockInputStream.h"

namespace DB
{

void ParallelParsingBlockInputStream::segmentatorThreadFunction()
{
    setThreadName("Segmentator");
    try
    {
        while (!is_cancelled && !is_exception_occured && !executed)
        {
            ++segmentator_ticket_number;
            const auto current_unit_number = segmentator_ticket_number % max_threads_to_use;
            auto & unit = processing_units[current_unit_number];

            {
                std::unique_lock lock(mutex);
                segmentator_condvar.wait(lock, [&]{ return unit.status == READY_TO_INSERT || is_exception_occured || executed; });
            }

            if (is_exception_occured)
                break;

            // Segmentating the original input.
            unit.segment.used_size = 0;

            //It returns bool, but it is useless
            const auto res = file_segmentation_engine(original_buffer, unit.segment.memory, unit.segment.used_size, min_chunk_size);

            if (!res)
            {
                unit.is_last = true;
                unit.status = READY_TO_PARSE;
                scheduleParserThreadForUnitWithNumber(current_unit_number);
                break;
            }

            // Creating buffer from the segment of data.
            auto new_buffer = BufferBase::Buffer(unit.segment.memory.data(),
                                                 unit.segment.memory.data() + unit.segment.used_size);

            unit.readbuffer->buffer().swap(new_buffer);
            unit.readbuffer->position() = unit.readbuffer->buffer().begin();

            unit.parser = std::make_unique<InputStreamFromInputFormat>(
                    input_processor_creator(*unit.readbuffer, header, context, row_input_format_params, format_settings)
            );

            unit.status = READY_TO_PARSE;
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

    auto & unit = processing_units[current_unit_number];

    try
    {
        {
            std::unique_lock lock(mutex);

            unit.block_ext.block.clear();
            unit.block_ext.block_missing_values.clear();

            if (unit.is_last || unit.readbuffer->position() == nullptr)
            {
                unit.block_ext.block.emplace_back(Block());
                unit.block_ext.block_missing_values.emplace_back(BlockMissingValues());
                unit.status = READY_TO_READ;
                reader_condvar.notify_all();
                return;
            }

        }

        //We don't know how many blocks will be. So we have to read them all until an empty block occured.
        while (true)
        {
            auto block = unit.parser->read();

            if (block == Block())
                break;

            unit.block_ext.block.emplace_back(block);
            unit.block_ext.block_missing_values.emplace_back(unit.parser->getMissingValues());
        }

        {
            std::unique_lock lock(mutex);
            unit.status = READY_TO_READ;
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
    if (isCancelledOrThrowIfKilled() || executed)
        return res;

    std::unique_lock lock(mutex);
    const auto current_unit_number = reader_ticket_number % max_threads_to_use;
    auto & unit = processing_units[current_unit_number];

    reader_condvar.wait(lock, [&](){ return unit.status == READY_TO_READ || is_exception_occured || executed; });

    /// Check for an exception and rethrow it
    if (is_exception_occured)
    {
        //LOG_TRACE(&Poco::Logger::get("ParallelParsingBLockInputStream::readImpl()"), "Exception occured. Will cancel the query.");
        lock.unlock();
        cancel(false);
        rethrowFirstException(exceptions);
    }

    res = std::move(unit.block_ext.block.at(internal_block_iter));
    last_block_missing_values = std::move(unit.block_ext.block_missing_values[internal_block_iter]);

    if (++internal_block_iter == unit.block_ext.block.size())
    {
        if (unit.is_last)
        {
            //In case that all data was read we don't need to cancel.
            executed= true;
            return res;
        }

        internal_block_iter = 0;
        ++reader_ticket_number;
        unit.status = READY_TO_INSERT;
        segmentator_condvar.notify_all();
    }

    return res;
}
}
