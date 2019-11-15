#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Interpreters/Context.h>

namespace DB
{

/**
 * ORDER-PRESERVING parallel parsing of data formats.
 * It splits original data into chunks. Then each chunk is parsed by different thread.
 * The number of chunks equals to max_threads_for_parallel_parsing setting.
 * The size of chunk is equal to min_chunk_size_for_parallel_parsing setting.
 *
 * This stream has three kinds of threads: one segmentator, multiple parsers
 * (max_threads_for_parallel_parsing) and one reader thread -- that is, the one
 * from which readImpl() is called.
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
            const Context & context,
            const RowInputFormatParams & params,
            const FormatSettings & settings)>;
public:
    struct InputCreatorParams
    {
        const Block &sample;
        const Context &context;
        const RowInputFormatParams& row_input_format_params;
        const FormatSettings &settings;
    };

    struct Builder
    {
        ReadBuffer & read_buffer;
        const InputProcessorCreator &input_processor_creator;
        const InputCreatorParams &input_creator_params;
        FormatFactory::FileSegmentationEngine file_segmentation_engine;
        size_t max_threads_to_use;
        size_t min_chunk_size;
    };

    explicit ParallelParsingBlockInputStream(const Builder & builder)
            : header(builder.input_creator_params.sample),
              context(builder.input_creator_params.context),
              row_input_format_params(builder.input_creator_params.row_input_format_params),
              format_settings(builder.input_creator_params.settings),
              input_processor_creator(builder.input_processor_creator),
              min_chunk_size(builder.min_chunk_size),
              original_buffer(builder.read_buffer),
              pool(builder.max_threads_to_use),
              file_segmentation_engine(builder.file_segmentation_engine)
    {
        // Allocate more units than threads to decrease segmentator
        // waiting on reader on wraparound. The number is random.
        for (size_t i = 0; i < builder.max_threads_to_use + 4; ++i)
            processing_units.emplace_back(builder);

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

        for (auto& unit: processing_units)
            unit.parser->cancel(kill);

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
    const Context context;
    const RowInputFormatParams row_input_format_params;
    const FormatSettings format_settings;
    const InputProcessorCreator input_processor_creator;

    const size_t min_chunk_size;

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

    struct MemoryExt
    {
        Memory<> memory;
        size_t used_size{0};
    };

    struct BlockExt
    {
        std::vector<Block> block;
        std::vector<BlockMissingValues> block_missing_values;
    };

    struct ProcessingUnit
    {
        explicit ProcessingUnit(const Builder & builder) : status(ProcessingUnitStatus::READY_TO_INSERT)
        {
            readbuffer = std::make_unique<ReadBuffer>(segment.memory.data(), segment.used_size, 0);
            parser = std::make_unique<InputStreamFromInputFormat>(
                builder.input_processor_creator(*readbuffer,
                    builder.input_creator_params.sample,
                    builder.input_creator_params.context,
                    builder.input_creator_params.row_input_format_params,
                    builder.input_creator_params.settings));
        }

        BlockExt block_ext;
        std::unique_ptr<ReadBuffer> readbuffer;
        MemoryExt segment;
        std::unique_ptr<InputStreamFromInputFormat> parser;
        std::atomic<ProcessingUnitStatus> status;
        bool is_last{false};
    };

    std::exception_ptr background_exception = nullptr;

    // We use deque instead of vector, because it does not require a move
    // constructor, which is absent for atomics that are inside ProcessingUnit.
    std::deque<ProcessingUnit> processing_units;


    void scheduleParserThreadForUnitWithNumber(size_t unit_number)
    {
        pool.scheduleOrThrowOnError(std::bind(&ParallelParsingBlockInputStream::parserThreadFunction, this, unit_number));
    }

    void finishAndWait()
    {
        finished = true;

        {
            std::unique_lock lock(mutex);
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
    void parserThreadFunction(size_t bucket_num);

    // Save/log a background exception, set termination flag, wake up all
    // threads. This function is used by segmentator and parsed threads.
    // readImpl() is called from the main thread, so the exception handling
    // is different.
    void onBackgroundException();
};

};
