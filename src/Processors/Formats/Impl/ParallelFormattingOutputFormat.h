#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Common/Arena.h>
#include <IO/WriteBufferFromArena.h>
#include <Formats/FormatFactory.h>

#include <deque>
#include <future>
#include <Common/setThreadName.h>

#include <atomic>
#include <Common/ThreadPool.h>
#include <Poco/Event.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

#include <common/logger_useful.h>
#include <Common/Exception.h>

namespace DB
{

/**
 * ORDER-PRESERVING parallel formatting of data formats.
 * The idea is similar to ParallelParsingInputFormat.
 * You add several Chunks through consume() method, each Chunk is formatted by some thread
 * in ThreadPool into a temporary buffer. (Formatting is being done in parallel.)
 * Then, another thread add temporary buffers into a "real" WriteBuffer.
 *
 *                   Formatters
 *      |   |   |   |   |   |   |   |   |   |
 *      v   v   v   v   v   v   v   v   v   v
 *    |---|---|---|---|---|---|---|---|---|---|
 *    | 1 | 2 | 3 | 4 | 5 | . | . | . | . | N | <-- Processing units
 *    |---|---|---|---|---|---|---|---|---|---|
 *      ^               ^
 *      |               |
 *   Collector       addChunk
 * 
 * There is a container of ProcessingUnits - internal entity, storing a Chunk to format,
 * a continuous memory buffer to store the formatted Chunk and some flags for synchronization needs.
 * Each ProcessingUnits has a unique number - the place in the container.
 * So, Chunk is added through addChunk method, which waits until current ProcessingUnit would be ready to insert
 * (ProcessingUnitStatus = READY_TO_INSERT), changes status to READY_TO_PARSE and spawns a new task in ThreadPool to parse it.
 * The other thread, we call it Collector waits until a ProcessingUnit which it points to would be READY_TO_READ.
 * Then it adds a temporary buffer to a real WriteBuffer.
 * Both Collector and a thread which adds Chunks have unit_number - a pointer to ProcessingUnit which they are aim to work with.
 * 
 * Note, that collector_unit_number is always less or equal to current_unit_number, that's why the formatting is order-preserving.
 * 
 * To stop the execution, a fake Chunk is added (ProcessingUnitType = FINALIZE) and finalize()
 * function is blocked until the Collector thread is done.
*/
class ParallelFormattingOutputFormat : public IOutputFormat
{
public:
    /// Used to recreate formatter on every new data piece.
    using InternalFormatterCreator = std::function<OutputFormatPtr(WriteBuffer & buf)>;

    /// Struct to simplify constructor.
    struct Params
    {
        WriteBuffer & out;
        const Block & header;
        InternalFormatterCreator internal_formatter_creator;
        const size_t max_threads_for_parallel_formatting;
    };

    ParallelFormattingOutputFormat() = delete;

    explicit ParallelFormattingOutputFormat(Params params)
        : IOutputFormat(params.header, params.out)
        , internal_formatter_creator(params.internal_formatter_creator)
        , pool(params.max_threads_for_parallel_formatting)

    {
        /// Just heuristic. We need one thread for collecting, one thread for receiving chunks
        /// and n threads for formatting.
        processing_units.resize(params.max_threads_for_parallel_formatting + 2);
        collector_thread = ThreadFromGlobalPool([&] { collectorThreadFunction(); });
        LOG_TRACE(&Poco::Logger::get("ParallelFormattingOutputFormat"), "Parallel formatting is being used.");
    }

    ~ParallelFormattingOutputFormat() override
    {
        finishAndWait();
    }

    String getName() const override { return "ParallelFormattingOutputFormat"; }

    void flush() override
    {
        need_flush = true;
    }

    void doWritePrefix() override
    {
        addChunk(Chunk{}, ProcessingUnitType::START, /*can_throw_exception*/ true);
    }

    void onCancel() override
    {
        finishAndWait();
    }

protected:
    void consume(Chunk chunk) override final
    {
        addChunk(std::move(chunk), ProcessingUnitType::PLAIN, /*can_throw_exception*/ true);
    }

    void consumeTotals(Chunk totals) override
    {
        addChunk(std::move(totals), ProcessingUnitType::TOTALS, /*can_throw_exception*/ true);
    }

    void consumeExtremes(Chunk extremes) override
    {
        addChunk(std::move(extremes), ProcessingUnitType::EXTREMES, /*can_throw_exception*/ true);
    }

    void finalize() override
    {
        need_flush = true;
        IOutputFormat::finalized = true;
        /// Don't throw any background_exception here, because we want to finalize the execution.
        /// Exception will be checked after main thread is finished.
        addChunk(Chunk{}, ProcessingUnitType::FINALIZE, /*can_throw_exception*/ false);
        collector_finished.wait();

        if (collector_thread.joinable())
            collector_thread.join();

        {
            std::unique_lock<std::mutex> lock(mutex);
            if (background_exception)
                std::rethrow_exception(background_exception);
        }
    }

private:
    InternalFormatterCreator internal_formatter_creator;

    /// Status to synchronize multiple threads.
    enum ProcessingUnitStatus
    {
        READY_TO_INSERT,
        READY_TO_FORMAT,
        READY_TO_READ
    };

    /// Some information about what methods to call from internal parser.
    enum class ProcessingUnitType
    {
        START,
        PLAIN,
        TOTALS,
        EXTREMES,
        FINALIZE
    };

    void addChunk(Chunk chunk, ProcessingUnitType type, bool can_throw_exception)
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            if (background_exception && can_throw_exception)
                std::rethrow_exception(background_exception);
        }

        const auto current_unit_number = writer_unit_number % processing_units.size();
        auto & unit = processing_units[current_unit_number];

        {
            std::unique_lock<std::mutex> lock(mutex);
            writer_condvar.wait(lock,
                [&]{ return unit.status == READY_TO_INSERT || emergency_stop; });
        }

        if (emergency_stop)
            return;

        assert(unit.status == READY_TO_INSERT);
        unit.chunk = std::move(chunk);
        /// Resize memory without deallocation.
        unit.segment.resize(0);
        unit.status = READY_TO_FORMAT;
        unit.type = type;

        scheduleFormatterThreadForUnitWithNumber(current_unit_number);

        ++writer_unit_number;
    }

    struct ProcessingUnit
    {
        std::atomic<ProcessingUnitStatus> status{ProcessingUnitStatus::READY_TO_INSERT};
        ProcessingUnitType type{ProcessingUnitType::START};
        Chunk chunk;
        Memory<> segment;
        size_t actual_memory_size{0};
    };

    Poco::Event collector_finished{};

    std::atomic_bool need_flush{false};

    // There are multiple "formatters", that's why we use thread pool.
    ThreadPool pool;
    // Collecting all memory to original ReadBuffer
    ThreadFromGlobalPool collector_thread;

    std::exception_ptr background_exception = nullptr;

    /// We use deque, because ProcessingUnit doesn't have move or copy constructor.
    std::deque<ProcessingUnit> processing_units;

    std::mutex mutex;
    std::atomic_bool emergency_stop{false};

    std::atomic_size_t collector_unit_number{0};
    std::atomic_size_t writer_unit_number{0};

    std::condition_variable collector_condvar;
    std::condition_variable writer_condvar;

    void finishAndWait()
    {
        emergency_stop = true;

        {
            std::unique_lock<std::mutex> lock(mutex);
            collector_condvar.notify_all();
            writer_condvar.notify_all();
        }

        if (collector_thread.joinable())
            collector_thread.join();

        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


    void onBackgroundException()
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (!background_exception)
        {
            background_exception = std::current_exception();
        }
        emergency_stop = true;
        writer_condvar.notify_all();
        collector_condvar.notify_all();
    }

    void scheduleFormatterThreadForUnitWithNumber(size_t ticket_number)
    {
        pool.scheduleOrThrowOnError([this, ticket_number] { formatterThreadFunction(ticket_number); });
    }

    void collectorThreadFunction()
    {
        setThreadName("Collector");

        try
        {
            while (!emergency_stop)
            {
                const auto current_unit_number = collector_unit_number % processing_units.size();
                auto & unit = processing_units[current_unit_number];

                {
                    std::unique_lock<std::mutex> lock(mutex);
                    collector_condvar.wait(lock,
                        [&]{ return unit.status == READY_TO_READ || emergency_stop; });
                }

                if (emergency_stop)
                    break;

                assert(unit.status == READY_TO_READ);

                /// Use this copy to after notification to stop the execution.
                auto copy_if_unit_type = unit.type;

                /// Do main work here.
                out.write(unit.segment.data(), unit.actual_memory_size);

                if (need_flush.exchange(false) || auto_flush)
                    IOutputFormat::flush();

                ++collector_unit_number;

                {
                    /// Notify other threads.
                    std::lock_guard<std::mutex> lock(mutex);
                    unit.status = READY_TO_INSERT;
                    writer_condvar.notify_all();
                }
                /// We can exit only after writing last piece of to out buffer.
                if (copy_if_unit_type == ProcessingUnitType::FINALIZE)
                {
                    break;
                }
            }
            collector_finished.set();
        }
        catch (...)
        {
            collector_finished.set();
            onBackgroundException();
        }
    }

    /// This function is executed in ThreadPool and the only purpose of it is to format one Chunk into a continuous buffer in memory.
    void formatterThreadFunction(size_t current_unit_number)
    {
        setThreadName("Formatter");

        try
        {
            auto & unit = processing_units[current_unit_number];
            assert(unit.status = READY_TO_FORMAT);

            /// We want to preallocate memory buffer (increase capacity)
            /// and put the pointer at the beginning of the buffer
            /// FIXME: Implement reserve() method in Memory.
            unit.segment.resize(DBMS_DEFAULT_BUFFER_SIZE);
            unit.segment.resize(0);

            unit.actual_memory_size = 0;
            BufferWithOutsideMemory<WriteBuffer> out_buffer(unit.segment);

            auto formatter = internal_formatter_creator(out_buffer);

            switch (unit.type)
            {
                case ProcessingUnitType::START :
                {
                    formatter->doWritePrefix();
                    break;
                }
                case ProcessingUnitType::PLAIN :
                {
                    formatter->consume(std::move(unit.chunk));
                    break;
                }
                case ProcessingUnitType::TOTALS :
                {
                    formatter->consumeTotals(std::move(unit.chunk));
                    break;
                }
                case ProcessingUnitType::EXTREMES :
                {
                    formatter->consumeExtremes(std::move(unit.chunk));
                    break;
                }
                case ProcessingUnitType::FINALIZE :
                {
                    formatter->doWriteSuffix();
                    break;
                }
            }
            /// Flush all the data to handmade buffer.
            formatter->flush();
            unit.actual_memory_size = out_buffer.getActualSize();

            {
                std::lock_guard<std::mutex> lock(mutex);
                unit.status = READY_TO_READ;
                collector_condvar.notify_all();
            }
        }
        catch (...)
        {
            onBackgroundException();
        }
    }
};

}
