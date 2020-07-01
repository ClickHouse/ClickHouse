#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>

namespace DB
{

/**
 * ORDER-PRESERVING parallel parsing of data formats.
 * It splits original data into chunks. Then each chunk is parsed by different thread.
 * The number of chunks equals to the number or parser threads.
 * The size of chunk is equal to min_chunk_bytes_for_parallel_parsing setting.
 *
 * This stream has three kinds of threads: one segmentator, multiple parsers,
 * and one reader thread -- that is, the one from which readImpl() is called.
 * They operate one after another on parts of data called "processing units".
 * One unit consists of buffer with raw data from file, filled by segmentator
 * thread. This raw data is then parsed by a parser thread to form a number of
 * Blocks. These Blocks are returned to the parent stream from readImpl().
 * After being read out, a processing unit is reused, to save on allocating
 * memory for the raw buffer. The processing units are organized into a circular
 * array to facilitate reuse and to apply backpressure on the segmentator thread
 * -- after it runs out of processing units, it has to wait for the reader to
 * read out the previous blocks.
 * The outline of what the threads do is as follows:
 * segmentator thread:
 *  1) wait for the next processing unit to become empty
 *  2) fill it with a part of input file
 *  3) start a parser thread
 *  4) repeat until eof
 * parser thread:
 *  1) parse the given raw buffer without any synchronization
 *  2) signal that the given unit is ready to read
 *  3) finish
 * readImpl():
 *  1) wait for the next processing unit to become ready to read
 *  2) take the blocks from the processing unit to return them to the caller
 *  3) signal that the processing unit is empty
 *  4) repeat until it encounters unit that is marked as "past_the_end"
 * All threads must also check for cancel/eof/exception flags.
 */
class ParallelParsingBlockInputStream : public IBlockInputStream
{
private:
    using ReadCallback = std::function<void()>;

    using InputProcessorCreator = std::function<InputFormatPtr(
            ReadBuffer & buf,
            const Block & header,
            const RowInputFormatParams & params,
            const FormatSettings & settings)>;
public:
    struct InputCreatorParams
    {
        const Block & sample;
        const RowInputFormatParams & row_input_format_params;
        const FormatSettings &settings;
    };

    struct Params
    {
        ReadBuffer & read_buffer;
        const InputProcessorCreator & input_processor_creator;
        const InputCreatorParams & input_creator_params;
        FormatFactory::FileSegmentationEngine file_segmentation_engine;
        int max_threads;
        size_t min_chunk_bytes;
    };

    explicit ParallelParsingBlockInputStream(const Params & params)
            : header(params.input_creator_params.sample),
              row_input_format_params(params.input_creator_params.row_input_format_params),
              format_settings(params.input_creator_params.settings),
              input_processor_creator(params.input_processor_creator),
              min_chunk_bytes(params.min_chunk_bytes),
              original_buffer(params.read_buffer),
              // Subtract one thread that we use for segmentation and one for
              // reading. After that, must have at least two threads left for
              // parsing. See the assertion below.
              pool(std::max(2, params.max_threads - 2)),
              file_segmentation_engine(params.file_segmentation_engine)
    {
        // See comment above.
        assert(params.max_threads >= 4);

        // One unit for each thread, including segmentator and reader, plus a
        // couple more units so that the segmentation thread doesn't spuriously
        // bump into reader thread on wraparound.
        processing_units.resize(params.max_threads + 2);

        segmentator_thread = ThreadFromGlobalPool([this] { segmentatorThreadFunction(); });
    }

    String getName() const override { return "ParallelParsing"; }

    ~ParallelParsingBlockInputStream() override
    {
        finishAndWait();
    }

    void cancel(bool kill) override
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
         * chunk size is on the order of megabytes, this should't be too long.
         * We can't call IInputFormat->cancel here, because the parser object is
         * local to the parser thread, and we don't want to introduce any
         * synchronization between parser threads and the other threads to get
         * better performance. An ideal solution would be to add a callback to
         * IInputFormat that checks whether it was cancelled.
         */

        finishAndWait();
    }

    Block getHeader() const override
    {
        return header;
    }

protected:
    //Reader routine
    Block readImpl() override;

    const BlockMissingValues & getMissingValues() const override
    {
        return last_block_missing_values;
    }

private:
    const Block header;
    const RowInputFormatParams row_input_format_params;
    const FormatSettings format_settings;
    const InputProcessorCreator input_processor_creator;

    const size_t min_chunk_bytes;

    /*
     * This is declared as atomic to avoid UB, because parser threads access it
     * without synchronization.
     */
    std::atomic<bool> finished{false};

    BlockMissingValues last_block_missing_values;

    // Original ReadBuffer to read from.
    ReadBuffer & original_buffer;

    //Non-atomic because it is used in one thread.
    std::optional<size_t> next_block_in_current_unit;
    size_t segmentator_ticket_number{0};
    size_t reader_ticket_number{0};

    std::mutex mutex;
    std::condition_variable reader_condvar;
    std::condition_variable segmentator_condvar;

    // There are multiple "parsers", that's why we use thread pool.
    ThreadPool pool;
    // Reading and segmentating the file
    ThreadFromGlobalPool segmentator_thread;

    // Function to segment the file. Then "parsers" will parse that segments.
    FormatFactory::FileSegmentationEngine file_segmentation_engine;

    enum ProcessingUnitStatus
    {
        READY_TO_INSERT,
        READY_TO_PARSE,
        READY_TO_READ
    };

    struct BlockExt
    {
        std::vector<Block> block;
        std::vector<BlockMissingValues> block_missing_values;
    };

    struct ProcessingUnit
    {
        explicit ProcessingUnit()
            : status(ProcessingUnitStatus::READY_TO_INSERT)
        {
        }

        BlockExt block_ext;
        Memory<> segment;
        std::atomic<ProcessingUnitStatus> status;
        bool is_last{false};
    };

    std::exception_ptr background_exception = nullptr;

    // We use deque instead of vector, because it does not require a move
    // constructor, which is absent for atomics that are inside ProcessingUnit.
    std::deque<ProcessingUnit> processing_units;


    void scheduleParserThreadForUnitWithNumber(size_t ticket_number)
    {
        pool.scheduleOrThrowOnError(std::bind(&ParallelParsingBlockInputStream::parserThreadFunction, this, ticket_number));
    }

    void finishAndWait()
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

    void segmentatorThreadFunction();
    void parserThreadFunction(size_t current_ticket_number);

    // Save/log a background exception, set termination flag, wake up all
    // threads. This function is used by segmentator and parsed threads.
    // readImpl() is called from the main thread, so the exception handling
    // is different.
    void onBackgroundException();
};

}
