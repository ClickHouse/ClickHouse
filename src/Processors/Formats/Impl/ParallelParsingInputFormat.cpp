#include <Processors/Formats/Impl/ParallelParsingInputFormat.h>
#include <IO/ReadHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <common/scope_guard_safe.h>

namespace DB
{

void ParallelParsingInputFormat::segmentatorThreadFunction(ThreadGroupStatusPtr thread_group)
{
    SCOPE_EXIT_SAFE(
        if (thread_group)
            CurrentThread::detachQueryIfNotDetached();
    );
    if (thread_group)
        CurrentThread::attachTo(thread_group);

    setThreadName("Segmentator");
    try
    {
        while (!parsing_finished)
        {
            const auto segmentator_unit_number = segmentator_ticket_number % processing_units.size();
            auto & unit = processing_units[segmentator_unit_number];

            {
                std::unique_lock<std::mutex> lock(mutex);
                segmentator_condvar.wait(lock,
                                         [&]{ return unit.status == READY_TO_INSERT || parsing_finished; });
            }

            if (parsing_finished)
                break;

            assert(unit.status == READY_TO_INSERT);

            // Segmentating the original input.
            unit.segment.resize(0);

            auto [have_more_data, currently_read_rows] = file_segmentation_engine(in, unit.segment, min_chunk_bytes);

            unit.offset = successfully_read_rows_count;
            successfully_read_rows_count += currently_read_rows;

            unit.is_last = !have_more_data;
            unit.status = READY_TO_PARSE;
            scheduleParserThreadForUnitWithNumber(segmentator_ticket_number);
            ++segmentator_ticket_number;

            if (!have_more_data)
                break;
        }
    }
    catch (...)
    {
        onBackgroundException(successfully_read_rows_count);
    }
}

void ParallelParsingInputFormat::parserThreadFunction(ThreadGroupStatusPtr thread_group, size_t current_ticket_number)
{
    SCOPE_EXIT_SAFE(
        if (thread_group)
            CurrentThread::detachQueryIfNotDetached();
    );
    if (thread_group)
        CurrentThread::attachTo(thread_group);

    const auto parser_unit_number = current_ticket_number % processing_units.size();
    auto & unit = processing_units[parser_unit_number];

    try
    {
        setThreadName("ChunkParser");

        /*
         * This is kind of suspicious -- the input_process_creator contract with
         * respect to multithreaded use is not clear, but we hope that it is
         * just a 'normal' factory class that doesn't have any state, and so we
         * can use it from multiple threads simultaneously.
         */
        ReadBuffer read_buffer(unit.segment.data(), unit.segment.size(), 0);

        InputFormatPtr input_format = internal_parser_creator(read_buffer);
        input_format->setCurrentUnitNumber(current_ticket_number);
        InternalParser parser(input_format);

        unit.chunk_ext.chunk.clear();
        unit.chunk_ext.block_missing_values.clear();

        /// Propagate column_mapping to other parsers.
        /// Note: column_mapping is used only for *WithNames types
        if (current_ticket_number != 0)
            input_format->setColumnMapping(column_mapping);

        // We don't know how many blocks will be. So we have to read them all
        // until an empty block occurred.
        Chunk chunk;
        while (!parsing_finished && (chunk = parser.getChunk()) != Chunk())
        {
            /// Variable chunk is moved, but it is not really used in the next iteration.
            /// NOLINTNEXTLINE(bugprone-use-after-move)
            unit.chunk_ext.chunk.emplace_back(std::move(chunk));
            unit.chunk_ext.block_missing_values.emplace_back(parser.getMissingValues());
        }

        /// Extract column_mapping from first parser to propagate it to others
        if (current_ticket_number == 0)
        {
            column_mapping = input_format->getColumnMapping();
            column_mapping->is_set = true;
            first_parser_finished.set();
        }

        // We suppose we will get at least some blocks for a non-empty buffer,
        // except at the end of file. Also see a matching assert in readImpl().
        assert(unit.is_last || !unit.chunk_ext.chunk.empty() || parsing_finished);

        std::lock_guard<std::mutex> lock(mutex);
        unit.status = READY_TO_READ;
        reader_condvar.notify_all();
    }
    catch (...)
    {
        onBackgroundException(unit.offset);
    }
}


void ParallelParsingInputFormat::onBackgroundException(size_t offset)
{
    std::unique_lock<std::mutex> lock(mutex);
    if (!background_exception)
    {
        background_exception = std::current_exception();
        if (ParsingException * e = exception_cast<ParsingException *>(background_exception))
            if (e->getLineNumber() != -1)
                e->setLineNumber(e->getLineNumber() + offset);
    }
    tryLogCurrentException(__PRETTY_FUNCTION__);
    parsing_finished = true;
    first_parser_finished.set();
    reader_condvar.notify_all();
    segmentator_condvar.notify_all();
}

Chunk ParallelParsingInputFormat::generate()
{
    /// Delayed launching of segmentator thread
    if (unlikely(!parsing_started.exchange(true)))
    {
        segmentator_thread = ThreadFromGlobalPool(
            &ParallelParsingInputFormat::segmentatorThreadFunction, this, CurrentThread::getGroup());
    }

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

    const auto inserter_unit_number = reader_ticket_number % processing_units.size();
    auto & unit = processing_units[inserter_unit_number];

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
