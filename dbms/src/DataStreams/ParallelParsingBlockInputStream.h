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

namespace DB
{


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

    ParallelParsingBlockInputStream(Builder builder)
            : max_threads_to_use(builder.max_threads_to_use),
              min_chunk_size(builder.min_chunk_size),
              original_buffer(builder.read_buffer),
              pool(builder.max_threads_to_use),
              file_segmentation_engine(builder.file_segmentation_engine)
    {
        segments.resize(max_threads_to_use);
        blocks.resize(max_threads_to_use);
        exceptions.resize(max_threads_to_use);
        buffers.reserve(max_threads_to_use);
        working_field.reserve(max_threads_to_use);

        for (size_t i = 0; i < max_threads_to_use; ++i)
        {
            buffers.emplace_back(std::make_unique<ReadBuffer>(segments[i].memory.data(), segments[i].used_size, 0));
            working_field.emplace_back(builder.input_processor_creator, builder.input_creator_params, segments[i], *buffers[i], blocks[i], exceptions[i]);
        }

        segmentator_thread = ThreadFromGlobalPool([this] { segmentatorThreadFunction(); });
    }

    String getName() const override { return "ParallelParsing"; }

    ~ParallelParsingBlockInputStream() override
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

    void cancel(bool kill) override
    {
        if (kill)
            is_killed = true;
        bool old_val = false;
        if (!is_cancelled.compare_exchange_strong(old_val, true))
            return;

        for (auto& unit: working_field)
            unit.reader->cancel(kill);

        {
            std::unique_lock lock(mutex);
            segmentator_condvar.notify_all();
            reader_condvar.notify_all();
        }

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

    Block getHeader() const override
    {
        return working_field.at(0).reader->getHeader();
    }

protected:
    void readPrefix() override {}

    //Reader routine
    Block readImpl() override;

    const BlockMissingValues & getMissingValues() const override
    {
        std::lock_guard missing_values_lock(missing_values_mutex);
        return block_missing_values;
    }

private:

    const std::atomic<size_t> max_threads_to_use;
    const size_t min_chunk_size;

    std::atomic<bool> is_exception_occured{false};

    BlockMissingValues block_missing_values;
    mutable std::mutex missing_values_mutex;

    // Original ReadBuffer to read from.
    ReadBuffer & original_buffer;

    //Non-atomic because it is used in one thread.
    size_t reader_ticket_number{0};
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


    struct ProcessingUnit
    {
        ProcessingUnit(const InputProcessorCreator & input_getter,
                       const InputCreatorParams & params,
                       MemoryExt & chunk_,
                       ReadBuffer & readbuffer_,
                       Block & block_,
                       std::exception_ptr & exception_) : chunk(chunk_), readbuffer(readbuffer_), block(block_), exception(exception_)
        {
            reader = std::make_shared<InputStreamFromInputFormat>(input_getter(readbuffer, params.sample, params.context, params.row_input_format_params, params.settings));
        }

        MemoryExt & chunk;
        ReadBuffer & readbuffer;
        Block & block;
        BlockInputStreamPtr reader;

        std::exception_ptr & exception;
        ProcessingUnitStatus status{READY_TO_INSERT};
        bool is_last_unit{false};
    };


    using Blocks = std::vector<Block>;
    using ReadBuffers = std::vector<std::unique_ptr<ReadBuffer>>;
    using Segments = std::vector<MemoryExt>;
    using ProcessingUnits = std::vector<ProcessingUnit>;

    Segments segments;
    ReadBuffers buffers;
    Blocks blocks;
    Exceptions exceptions;

    ProcessingUnits working_field;

    void scheduleParserThreadForUnitWithNumber(size_t unit_number)
    {
        pool.schedule(std::bind(&ParallelParsingBlockInputStream::parserThreadFunction, this, unit_number));
    }

    void segmentatorThreadFunction();
    void parserThreadFunction(size_t bucket_num);
};

};



