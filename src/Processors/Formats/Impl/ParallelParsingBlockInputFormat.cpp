#include <Processors/Formats/Impl/ParallelParsingBlockInputFormat.h>

namespace DB
{

void ParallelParsingBlockInputFormat::segmentatorThreadFunction()
{
    setThreadName("Segmentator");
    try
    {
        while (!parsing_finished)
        {
            const auto current_unit_number = segmentator_ticket_number % processing_units.size();
            auto & unit = processing_units[current_unit_number];

            {
                std::unique_lock<std::mutex> lock(mutex);
                segmentator_condvar.wait(lock,
                                         [&]{ return unit.status == READY_TO_INSERT || parsing_finished; });
            }

            if (parsing_finished)
            {
                break;
            }

            assert(unit.status == READY_TO_INSERT);

            // Segmentating the original input.
            unit.segment.resize(0);

            const bool have_more_data = file_segmentation_engine(in, unit.segment, min_chunk_bytes);

            unit.is_last = !have_more_data;
            unit.status = READY_TO_PARSE;
            scheduleParserThreadForUnitWithNumber(segmentator_ticket_number);
            ++segmentator_ticket_number;

            if (!have_more_data)
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

void ParallelParsingBlockInputFormat::parserThreadFunction(size_t current_ticket_number)
{
    try
    {
        setThreadName("ChunkParser");

        const auto current_unit_number = current_ticket_number % processing_units.size();
        auto & unit = processing_units[current_unit_number];

        /*
         * This is kind of suspicious -- the input_process_creator contract with
         * respect to multithreaded use is not clear, but we hope that it is
         * just a 'normal' factory class that doesn't have any state, and so we
         * can use it from multiple threads simultaneously.
         */
        ReadBuffer read_buffer(unit.segment.data(), unit.segment.size(), 0);
        InputFormatPtr input_format = internal_parser_creator(read_buffer);
        InternalParser parser(input_format);

        unit.chunk_ext.chunk.clear();
        unit.chunk_ext.block_missing_values.clear();

        // We don't know how many blocks will be. So we have to read them all
        // until an empty block occured.
        Chunk chunk;
        while (!parsing_finished && (chunk = parser.getChunk()) != Chunk())
        {
            unit.chunk_ext.chunk.emplace_back(std::move(chunk));
            unit.chunk_ext.block_missing_values.emplace_back(parser.getMissingValues());
        }

        // We suppose we will get at least some blocks for a non-empty buffer,
        // except at the end of file. Also see a matching assert in readImpl().
        assert(unit.is_last || !unit.chunk_ext.chunk.empty());

        std::lock_guard<std::mutex> lock(mutex);
        unit.status = READY_TO_READ;
        reader_condvar.notify_all();
    }
    catch (...)
    {
        onBackgroundException();
    }
}


void ParallelParsingBlockInputFormat::onBackgroundException()
{
    tryLogCurrentException(__PRETTY_FUNCTION__);

    std::unique_lock<std::mutex> lock(mutex);
    if (!background_exception)
    {
        background_exception = std::current_exception();
    }
    parsing_finished = true;
    reader_condvar.notify_all();
    segmentator_condvar.notify_all();
}

Chunk ParallelParsingBlockInputFormat::generate()
{
    if (isCancelled() || parsing_finished)
    {
        /**
          * Check for background exception and rethrow it before we return.
          */
        std::unique_lock<std::mutex> lock(mutex);
        if (background_exception)
        {
            lock.unlock();
            onCancel();
            std::rethrow_exception(background_exception);
        }

        return {};
    }

    const auto current_unit_number = reader_ticket_number % processing_units.size();
    auto & unit = processing_units[current_unit_number];

    if (!next_block_in_current_unit.has_value())
    {
        // We have read out all the Blocks from the previous Processing Unit,
        // wait for the current one to become ready.
        std::unique_lock<std::mutex> lock(mutex);
        reader_condvar.wait(lock, [&](){ return unit.status == READY_TO_READ || parsing_finished; });

        if (parsing_finished)
        {
            /**
              * Check for background exception and rethrow it before we return.
              */
            if (background_exception)
            {
                lock.unlock();
                cancel();
                std::rethrow_exception(background_exception);
            }

            return {};
        }

        assert(unit.status == READY_TO_READ);
        next_block_in_current_unit = 0;
    }

    if (unit.chunk_ext.chunk.empty())
    {
        /*
         * Can we get zero blocks for an entire segment, when the format parser
         * skips it entire content and does not create any blocks? Probably not,
         * but if we ever do, we should add a loop around the above if, to skip
         * these. Also see a matching assert in the parser thread.
         */
        assert(unit.is_last);
        parsing_finished = true;
        return {};
    }

    assert(next_block_in_current_unit.value() < unit.chunk_ext.chunk.size());

    Chunk res = std::move(unit.chunk_ext.chunk.at(*next_block_in_current_unit));
    last_block_missing_values = std::move(unit.chunk_ext.block_missing_values[*next_block_in_current_unit]);

    next_block_in_current_unit.value() += 1;

    if (*next_block_in_current_unit == unit.chunk_ext.chunk.size())
    {
        // parsing_finished reading this Processing Unit, move to the next one.
        next_block_in_current_unit.reset();
        ++reader_ticket_number;

        if (unit.is_last)
        {
            // It it was the last unit, we're parsing_finished.
            parsing_finished = true;
        }
        else
        {
            // Pass the unit back to the segmentator.
            std::unique_lock<std::mutex> lock(mutex);
            unit.status = READY_TO_INSERT;
            segmentator_condvar.notify_all();
        }
    }

    return res;
}


}
