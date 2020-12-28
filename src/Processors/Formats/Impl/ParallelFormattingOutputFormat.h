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
        need_flush = true;
        if (!IOutputFormat::finalized) 
            finalize();
        finishAndWait();
    }

    String getName() const override { return "ParallelFormattingOutputFormat"; }

    void flush() override
    {
        need_flush = true;
    }

    void doWritePrefix() override 
    {
        addChunk(Chunk{}, ProcessingUnitType::START);
    }

    void onCancel() override 
    {
        finishAndWait();
    }

protected:
    void consume(Chunk chunk) override final
    {
        addChunk(std::move(chunk), ProcessingUnitType::PLAIN);
    }

    void consumeTotals(Chunk totals) override
    {
        addChunk(std::move(totals), ProcessingUnitType::TOTALS);
    }

    void consumeExtremes(Chunk extremes) override
    {
        addChunk(std::move(extremes), ProcessingUnitType::EXTREMES);
    }

    void finalize() override
    {
        IOutputFormat::finalized = true;
        addChunk(Chunk{}, ProcessingUnitType::FINALIZE);
        collector_finished.wait();
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

    void addChunk(Chunk chunk, ProcessingUnitType type)
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            if (background_exception)
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


    void formatterThreadFunction(size_t current_unit_number)
    {
        setThreadName("Formatter");

        try
        {
            auto & unit = processing_units[current_unit_number];
            assert(unit.status = READY_TO_FORMAT);

            unit.segment.resize(DBMS_DEFAULT_BUFFER_SIZE);
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
