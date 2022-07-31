#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Interpreters/Context.h>
#include <base/logger_useful.h>
#include <Poco/Event.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class Context;

/**
 * ORDER-PRESERVING parallel parsing of data formats.
 * It splits original data into chunks. Then each chunk is parsed by different thread.
 * The number of chunks equals to the number or parser threads.
 * The size of chunk is equal to min_chunk_bytes_for_parallel_parsing setting.
 *
 *                    Parsers
 *      |   |   |   |   |   |   |   |   |   |
 *      v   v   v   v   v   v   v   v   v   v
 *    |---|---|---|---|---|---|---|---|---|---|
 *    | 1 | 2 | 3 | 4 | 5 | . | . | . | . | N | <-- Processing units
 *    |---|---|---|---|---|---|---|---|---|---|
 *      ^               ^
 *      |               |
 *   readImpl        Segmentator
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
class ParallelParsingInputFormat : public IInputFormat
{
public:
    /* Used to recreate parser on every new data piece.*/
    using InternalParserCreator = std::function<InputFormatPtr(ReadBuffer & buf)>;

    struct Params
    {
        ReadBuffer & in;
        Block header;
        InternalParserCreator internal_parser_creator;
        FormatFactory::FileSegmentationEngine file_segmentation_engine;
        String format_name;
        size_t max_threads;
        size_t min_chunk_bytes;
        bool is_server;
    };

    explicit ParallelParsingInputFormat(Params params)
        : IInputFormat(std::move(params.header), params.in)
        , internal_parser_creator(params.internal_parser_creator)
        , file_segmentation_engine(params.file_segmentation_engine)
        , format_name(params.format_name)
        , min_chunk_bytes(params.min_chunk_bytes)
        , is_server(params.is_server)
        , pool(params.max_threads)
    {
        // One unit for each thread, including segmentator and reader, plus a
        // couple more units so that the segmentation thread doesn't spuriously
        // bump into reader thread on wraparound.
        processing_units.resize(params.max_threads + 2);

        LOG_TRACE(&Poco::Logger::get("ParallelParsingInputFormat"), "Parallel parsing is used");
    }

    ~ParallelParsingInputFormat() override
    {
        finishAndWait();
    }

    void resetParser() override final
    {
        throw Exception("resetParser() is not allowed for " + getName(), ErrorCodes::LOGICAL_ERROR);
    }

    const BlockMissingValues & getMissingValues() const override final
    {
        return last_block_missing_values;
    }

    String getName() const override final { return "ParallelParsingBlockInputFormat"; }

private:

    Chunk generate() override final;

    void onCancel() override final
    {
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

    class InternalParser
    {
    public:
        explicit InternalParser(const InputFormatPtr & input_format_)
        : input_format(input_format_)
        , port(input_format->getPort().getHeader(), input_format.get())
        {
            connect(input_format->getPort(), port);
            port.setNeeded();
        }

        Chunk getChunk()
        {
            while (true)
            {
                IProcessor::Status status = input_format->prepare();
                switch (status)
                {
                    case IProcessor::Status::Ready:
                        input_format->work();
                        break;

                    case IProcessor::Status::Finished:
                        return {};

                    case IProcessor::Status::PortFull:
                        return port.pull();

                    case IProcessor::Status::NeedData: break;
                    case IProcessor::Status::Async: break;
                    case IProcessor::Status::ExpandPipeline:
                        throw Exception("One of the parsers returned status " + IProcessor::statusToName(status) +
                                             " during parallel parsing", ErrorCodes::LOGICAL_ERROR);
                }
            }
        }

        const BlockMissingValues & getMissingValues() const { return input_format->getMissingValues(); }

    private:
        const InputFormatPtr & input_format;
        InputPort port;
    };

    const InternalParserCreator internal_parser_creator;
    /// Function to segment the file. Then "parsers" will parse that segments.
    FormatFactory::FileSegmentationEngine file_segmentation_engine;
    const String format_name;
    const size_t min_chunk_bytes;

    BlockMissingValues last_block_missing_values;

    /// Non-atomic because it is used in one thread.
    std::optional<size_t> next_block_in_current_unit;
    size_t segmentator_ticket_number{0};
    size_t reader_ticket_number{0};

    /// Mutex for internal synchronization between threads
    std::mutex mutex;

    /// finishAndWait can be called concurrently from
    /// multiple threads. Atomic flag is not enough
    /// because if finishAndWait called before destructor it can check the flag
    /// and destroy object immediately.
    std::mutex finish_and_wait_mutex;
    /// We don't use parsing_finished flag because it can be setup from multiple
    /// place in code. For example in case of bad data. It doesn't mean that we
    /// don't need to finishAndWait our class.
    bool finish_and_wait_called = false;

    std::condition_variable reader_condvar;
    std::condition_variable segmentator_condvar;

    Poco::Event first_parser_finished;

    std::atomic<bool> parsing_started{false};
    std::atomic<bool> parsing_finished{false};

    const bool is_server;

    /// There are multiple "parsers", that's why we use thread pool.
    ThreadPool pool;
    /// Reading and segmentating the file
    ThreadFromGlobalPool segmentator_thread;

    enum ProcessingUnitStatus
    {
        READY_TO_INSERT,
        READY_TO_PARSE,
        READY_TO_READ
    };

    struct ChunkExt
    {
        std::vector<Chunk> chunk;
        std::vector<BlockMissingValues> block_missing_values;
    };

    struct ProcessingUnit
    {
        ProcessingUnit()
            : status(ProcessingUnitStatus::READY_TO_INSERT)
        {
        }

        ChunkExt chunk_ext;
        Memory<> segment;
        std::atomic<ProcessingUnitStatus> status;
        /// Needed for better exception message.
        size_t offset = 0;
        bool is_last{false};
    };

    std::exception_ptr background_exception = nullptr;

    /// We use deque instead of vector, because it does not require a move
    /// constructor, which is absent for atomics that are inside ProcessingUnit.
    std::deque<ProcessingUnit> processing_units;

    /// Compute it to have a more understandable error message.
    size_t successfully_read_rows_count{0};


    void scheduleParserThreadForUnitWithNumber(size_t ticket_number)
    {
        pool.scheduleOrThrowOnError([this, ticket_number, group = CurrentThread::getGroup()]()
        {
            parserThreadFunction(group, ticket_number);
        });
        /// We have to wait here to possibly extract ColumnMappingPtr from the first parser.
        if (ticket_number == 0)
            first_parser_finished.wait();
    }

    void finishAndWait()
    {
        /// Defending concurrent segmentator thread join
        std::lock_guard finish_and_wait_lock(finish_and_wait_mutex);

        /// We shouldn't execute this logic twice
        if (finish_and_wait_called)
            return;

        finish_and_wait_called = true;

        /// Signal background threads to finish
        parsing_finished = true;

        {
            /// Additionally notify condvars
            std::lock_guard<std::mutex> lock(mutex);
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

    void segmentatorThreadFunction(ThreadGroupStatusPtr thread_group);
    void parserThreadFunction(ThreadGroupStatusPtr thread_group, size_t current_ticket_number);

    /// Save/log a background exception, set termination flag, wake up all
    /// threads. This function is used by segmentator and parsed threads.
    /// readImpl() is called from the main thread, so the exception handling
    /// is different.
    void onBackgroundException(size_t offset);
};

}
