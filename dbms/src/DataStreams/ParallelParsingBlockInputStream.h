#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/SharedReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Interpreters/Context.h>

namespace DB
{

/**
 * ORDER-PRESERVING parallel parsing of data formats.
 * It splits original data into chunks. Then each chunk is parsed by different thread.
 * The number of chunks equals to max_threads_for_parallel_reading setting.
 * The size of chunk is equal to min_chunk_size_for_parallel_reading setting.
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
        ReadBuffer &read_buffer;
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
        segments.resize(max_threads_to_use);
        blocks.resize(max_threads_to_use);
        exceptions.resize(max_threads_to_use);
        buffers.reserve(max_threads_to_use);
        readers.reserve(max_threads_to_use);
        is_last.assign(max_threads_to_use, false);

        for (size_t i = 0; i < max_threads_to_use; ++i)
        {
            status.emplace_back(ProcessingUnitStatus::READY_TO_INSERT);
            buffers.emplace_back(std::make_unique<ReadBuffer>(segments[i].memory.data(), segments[i].used_size, 0));
            readers.emplace_back(std::make_unique<InputStreamFromInputFormat>(builder.input_processor_creator(*buffers[i],
                    builder.input_creator_params.sample,
                    builder.input_creator_params.context,
                    builder.input_creator_params.row_input_format_params,
                    builder.input_creator_params.settings)));
        }

        segmentator_thread = ThreadFromGlobalPool([this] { segmentatorThreadFunction(); });
    }

    String getName() const override { return "ParallelParsing"; }

    ~ParallelParsingBlockInputStream() override
    {
        waitForAllThreads();
    }

    void cancel(bool kill) override
    {
        if (kill)
            is_killed = true;
        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true))
            return;

        for (auto& reader: readers)
            if (!reader->isCancelled())
                reader->cancel(kill);

        waitForAllThreads();
    }

    Block getHeader() const override
    {
        return header;
    }

protected:
    void readPrefix() override {}

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

    using Blocks = std::vector<BlockExt>;
    using ReadBuffers = std::vector<std::unique_ptr<ReadBuffer>>;
    using Segments = std::vector<MemoryExt>;
    using Status = std::deque<std::atomic<ProcessingUnitStatus>>;
    using InputStreamFromInputFormats = std::vector<std::unique_ptr<InputStreamFromInputFormat>>;

    //We cannot use std::vector<bool> because it is equal to bitset (which stores 8 bool in one byte).
    //That's why dataraces occured.
    using IsLastFlags = std::vector<char>;

    Segments segments;
    ReadBuffers buffers;
    Blocks blocks;
    Exceptions exceptions;
    Status status;
    InputStreamFromInputFormats readers;
    IsLastFlags is_last;

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
