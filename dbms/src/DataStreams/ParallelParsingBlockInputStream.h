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
              max_threads_to_use(builder.max_threads_to_use),
              min_chunk_size(builder.min_chunk_size),
              original_buffer(builder.read_buffer),
              pool(builder.max_threads_to_use),
              file_segmentation_engine(builder.file_segmentation_engine)
    {
        //LOG_TRACE(&Poco::Logger::get("ParallelParsingBLockInputStream()"), "Constructor");

        for (size_t i = 0; i < max_threads_to_use; ++i)
            processing_units.emplace_back(builder);

        segmentator_thread = ThreadFromGlobalPool([this] { segmentatorThreadFunction(); });
    }

    String getName() const override { return "ParallelParsing"; }

    ~ParallelParsingBlockInputStream() override
    {
        executed = true;
        waitForAllThreads();
    }

    void cancel(bool kill) override
    {
        //LOG_TRACE(&Poco::Logger::get("ParallelParsingBLockInputStream::cancel()"), "Try to cancel.");
        if (kill)
            is_killed = true;
        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true))
            return;

        executed = true;

        for (auto& unit: processing_units)
            if (!unit.parser->isCancelled())
                unit.parser->cancel(kill);

        waitForAllThreads();
        //LOG_TRACE(&Poco::Logger::get("ParallelParsingBLockInputStream::cancel()"), "Cancelled succsessfully.");
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

    const std::atomic<size_t> max_threads_to_use;
    const size_t min_chunk_size;

    std::atomic<bool> is_exception_occured{false};
    std::atomic<bool> executed{false};

    BlockMissingValues last_block_missing_values;

    // Original ReadBuffer to read from.
    ReadBuffer & original_buffer;

    //Non-atomic because it is used in one thread.
    size_t reader_ticket_number{1};
    size_t internal_block_iter{0};
    size_t segmentator_ticket_number{0};

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
            parser = std::make_unique<InputStreamFromInputFormat>(builder.input_processor_creator(*readbuffer,
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
        char is_last{false};
    };

    Exceptions exceptions;
    std::deque<ProcessingUnit> processing_units;

    void scheduleParserThreadForUnitWithNumber(size_t unit_number)
    {
        pool.scheduleOrThrowOnError(std::bind(&ParallelParsingBlockInputStream::parserThreadFunction, this, unit_number));
    }

    void waitForAllThreads()
    {
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
};

};
