#pragma once

#include <Processors/Formats/IOutputFormat.h>

#include <Common/Arena.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include "IO/WriteBufferFromString.h"
#include <Formats/FormatFactory.h>
#include <Poco/Event.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromArena.h>

#include <deque>
#include <atomic>

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
        LOG_TRACE(&Poco::Logger::get("ParallelFormattingOutputFormat"), "Parallel formatting is being used");
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

    /// There are no formats which support parallel formatting and progress writing at the same time
    void onProgress(const Progress &) override {}

    String getContentType() const override
    {
        WriteBufferFromOwnString buffer;
        return internal_formatter_creator(buffer)->getContentType();
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

    void finalize() override;

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

    void addChunk(Chunk chunk, ProcessingUnitType type, bool can_throw_exception);

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

    void finishAndWait();

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

    /// Collects all temporary buffers into main WriteBuffer.
    void collectorThreadFunction();

    /// This function is executed in ThreadPool and the only purpose of it is to format one Chunk into a continuous buffer in memory.
    void formatterThreadFunction(size_t current_unit_number);
};

}
