#include <DataStreams/ParallelParsingBlockInputStream.h>
#include <IO/ReadBuffer.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <ext/scope_guard.h>

namespace DB
{

ParallelParsingBlockInputStream::ParallelParsingBlockInputStream(const Params & params)
    : header(params.input_creator_params.sample),
      row_input_format_params(params.input_creator_params.row_input_format_params),
      format_settings(params.input_creator_params.settings),
      input_processor_creator(params.input_processor_creator),
      min_chunk_bytes(params.min_chunk_bytes),
      original_buffer(params.read_buffer),
      // Subtract one thread that we use for segmentation and one for
      // reading. After that, must have at least two threads left for
      // parsing. See the assertion below.
      pool(std::max(2, static_cast<int>(params.max_threads) - 2)),
      file_segmentation_engine(params.file_segmentation_engine)
{
    // See comment above.
    assert(params.max_threads >= 4);

    // One unit for each thread, including segmentator and reader, plus a
    // couple more units so that the segmentation thread doesn't spuriously
    // bump into reader thread on wraparound.
    processing_units.resize(params.max_threads + 2);

    segmentator_thread = ThreadFromGlobalPool(
        &ParallelParsingBlockInputStream::segmentatorThreadFunction, this, CurrentThread::getGroup());
}

ParallelParsingBlockInputStream::~ParallelParsingBlockInputStream()
{
    finishAndWait();
}

void ParallelParsingBlockInputStream::cancel(bool kill)
{
    /**
      * Can be called multiple times, from different threads. Saturate the
      * the kill flag with OR.
      */
    if (kill)
        is_killed = true;
    is_cancelled = true;

    /*
     * The format parsers themselves are not being cancelled here, so we'll
     * have to wait until they process the current block. Given that the
     * chunk size is on the order of megabytes, this shouldn't be too long.
     * We can't call IInputFormat->cancel here, because the parser object is
     * local to the parser thread, and we don't want to introduce any
     * synchronization between parser threads and the other threads to get
     * better performance. An ideal solution would be to add a callback to
     * IInputFormat that checks whether it was cancelled.
     */

    finishAndWait();
}

void ParallelParsingBlockInputStream::scheduleParserThreadForUnitWithNumber(size_t ticket_number)
{
    pool.scheduleOrThrowOnError([this, ticket_number, group = CurrentThread::getGroup()]()
    {
        parserThreadFunction(group, ticket_number);
    });
}

void ParallelParsingBlockInputStream::finishAndWait()
{
    finished = true;

    {
        std::unique_lock<std::mutex> lock(mutex);
        segmentator_condvar.notify_all();
        reader_condvar.notify_all();
    }

    if (segmentator_thread.joinable())
        segmentator_thread.join();

    try
    {
        pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void ParallelParsingBlockInputStream::segmentatorThreadFunction(ThreadGroupStatusPtr thread_group)
{
    SCOPE_EXIT(
        if (thread_group)
            CurrentThread::detachQueryIfNotDetached();
    );
    if (thread_group)
        CurrentThread::attachTo(thread_group);

    setThreadName("Segmentator");

    try
    {
        while (!finished)
        {
            const auto current_unit_number = segmentator_ticket_number % processing_units.size();
            auto & unit = processing_units[current_unit_number];

            {
                std::unique_lock<std::mutex> lock(mutex);
                segmentator_condvar.wait(lock,
                    [&]{ return unit.status == READY_TO_INSERT || finished; });
            }

            if (finished)
            {
                break;
            }

            assert(unit.status == READY_TO_INSERT);

            // Segmentating the original input.
            unit.segment.resize(0);

            const bool have_more_data = file_segmentation_engine(original_buffer,
                unit.segment, min_chunk_bytes);

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

void ParallelParsingBlockInputStream::parserThreadFunction(ThreadGroupStatusPtr thread_group, size_t current_ticket_number)
{
    SCOPE_EXIT(
        if (thread_group)
            CurrentThread::detachQueryIfNotDetached();
    );
    if (thread_group)
        CurrentThread::attachTo(thread_group);

    setThreadName("ChunkParser");

    try
    {
        const auto current_unit_number = current_ticket_number % processing_units.size();
        auto & unit = processing_units[current_unit_number];

        /*
         * This is kind of suspicious -- the input_process_creator contract with
         * respect to multithreaded use is not clear, but we hope that it is
         * just a 'normal' factory class that doesn't have any state, and so we
         * can use it from multiple threads simultaneously.
         */
        ReadBuffer read_buffer(unit.segment.data(), unit.segment.size(), 0);
        auto format = input_processor_creator(read_buffer, header, row_input_format_params, format_settings);
        format->setCurrentUnitNumber(current_ticket_number);
        auto parser = std::make_unique<InputStreamFromInputFormat>(std::move(format));

        unit.block_ext.block.clear();
        unit.block_ext.block_missing_values.clear();

        // We don't know how many blocks will be. So we have to read them all
        // until an empty block occurred.
        Block block;
        while (!finished && (block = parser->read()) != Block())
        {
            unit.block_ext.block.emplace_back(block);
            unit.block_ext.block_missing_values.emplace_back(parser->getMissingValues());
        }

        // We suppose we will get at least some blocks for a non-empty buffer,
        // except at the end of file. Also see a matching assert in readImpl().
        assert(unit.is_last || !unit.block_ext.block.empty());

        std::unique_lock<std::mutex> lock(mutex);
        unit.status = READY_TO_READ;
        reader_condvar.notify_all();
    }
    catch (...)
    {
        onBackgroundException();
    }
}

void ParallelParsingBlockInputStream::onBackgroundException()
{
    tryLogCurrentException(__PRETTY_FUNCTION__);

    std::unique_lock<std::mutex> lock(mutex);
    if (!background_exception)
    {
        background_exception = std::current_exception();
    }
    finished = true;
    reader_condvar.notify_all();
    segmentator_condvar.notify_all();
}

Block ParallelParsingBlockInputStream::readImpl()
{
    if (isCancelledOrThrowIfKilled() || finished)
    {
        /**
          * Check for background exception and rethrow it before we return.
          */
        std::unique_lock<std::mutex> lock(mutex);
        if (background_exception)
        {
            lock.unlock();
            cancel(false);
            std::rethrow_exception(background_exception);
        }

        return Block{};
    }

    const auto current_unit_number = reader_ticket_number % processing_units.size();
    auto & unit = processing_units[current_unit_number];

    if (!next_block_in_current_unit.has_value())
    {
        // We have read out all the Blocks from the previous Processing Unit,
        // wait for the current one to become ready.
        std::unique_lock<std::mutex> lock(mutex);
        reader_condvar.wait(lock, [&](){ return unit.status == READY_TO_READ || finished; });

        if (finished)
        {
            /**
              * Check for background exception and rethrow it before we return.
              */
            if (background_exception)
            {
                lock.unlock();
                cancel(false);
                std::rethrow_exception(background_exception);
            }

            return Block{};
        }

        assert(unit.status == READY_TO_READ);
        next_block_in_current_unit = 0;
    }

    if (unit.block_ext.block.empty())
    {
        /*
         * Can we get zero blocks for an entire segment, when the format parser
         * skips it entire content and does not create any blocks? Probably not,
         * but if we ever do, we should add a loop around the above if, to skip
         * these. Also see a matching assert in the parser thread.
         */
        assert(unit.is_last);
        finished = true;
        return Block{};
    }

    assert(next_block_in_current_unit.value() < unit.block_ext.block.size());

    Block res = std::move(unit.block_ext.block.at(*next_block_in_current_unit));
    last_block_missing_values = std::move(unit.block_ext.block_missing_values[*next_block_in_current_unit]);

    next_block_in_current_unit.value() += 1;

    if (*next_block_in_current_unit == unit.block_ext.block.size())
    {
        // Finished reading this Processing Unit, move to the next one.
        next_block_in_current_unit.reset();
        ++reader_ticket_number;

        if (unit.is_last)
        {
            // It it was the last unit, we're finished.
            finished = true;
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
