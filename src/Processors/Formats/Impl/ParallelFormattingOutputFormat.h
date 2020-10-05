#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Common/Arena.h>
#include <IO/WriteBufferFromArena.h>
#include <Formats/FormatFactory.h>

#include <deque>
#include <Common/setThreadName.h>

#include <atomic>
#include <Common/ThreadPool.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{

class ParallelFormattingOutputFormat : public IOutputFormat
{
public:
    /* Used to recreate formatter on every new data piece. */
    using InternalFormatterCreator = std::function<OutputFormatPtr(WriteBuffer & buf)>;

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
        processing_units.resize(params.max_threads_for_parallel_formatting + 2);

        collector_thread = ThreadFromGlobalPool([&] { collectorThreadFunction(); });
    }

    ~ParallelFormattingOutputFormat() override
    {
        flush();
        finishAndWait();
    }

    String getName() const override { return "ParallelFormattingOutputFormat"; }

    void flush() override
    {
        need_flush = true;
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
    }

private:

    InternalFormatterCreator internal_formatter_creator;

    enum ProcessingUnitStatus
    {
        READY_TO_INSERT,
        READY_TO_FORMAT,
        READY_TO_READ
    };


    enum class ProcessingUnitType
    {
        PLAIN,
        TOTALS,
        EXTREMES,
        FINALIZE
    };

    void addChunk(Chunk chunk, ProcessingUnitType type)
    {
        const auto current_unit_number = writer_unit_number % processing_units.size();

        auto & unit = processing_units[current_unit_number];

        {
            std::unique_lock<std::mutex> lock(mutex);
            writer_condvar.wait(lock,
                [&]{ return unit.status == READY_TO_INSERT || formatting_finished; });
        }

        assert(unit.status == READY_TO_INSERT);

        unit.chunk = std::move(chunk);

        /// Resize memory without deallocate
        unit.segment.resize(0);
        unit.status = READY_TO_FORMAT;
        unit.type = type;

        scheduleFormatterThreadForUnitWithNumber(current_unit_number);

        ++writer_unit_number;
    }

    struct ProcessingUnit
    {
        explicit ProcessingUnit()
            : status(ProcessingUnitStatus::READY_TO_INSERT)
        {
        }

        std::atomic<ProcessingUnitStatus> status;
        ProcessingUnitType type;
        Chunk chunk;
        Memory<> segment;
        size_t actual_memory_size{0};
        
    };

    std::atomic_bool need_flush{false};

    // There are multiple "formatters", that's why we use thread pool.
    ThreadPool pool;
    // Collecting all memory to original ReadBuffer
    ThreadFromGlobalPool collector_thread;

    std::exception_ptr background_exception = nullptr;

    std::deque<ProcessingUnit> processing_units;

    std::mutex mutex;
    std::atomic_bool formatting_finished{false};

    std::atomic_size_t collector_unit_number{0};
    std::atomic_size_t writer_unit_number{0};

    std::condition_variable collector_condvar;
    std::condition_variable writer_condvar;

    void finishAndWait()
    {
        formatting_finished = true;

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
        tryLogCurrentException(__PRETTY_FUNCTION__);

        std::unique_lock<std::mutex> lock(mutex);
        if (!background_exception)
        {
            background_exception = std::current_exception();
        }
        formatting_finished = true;
        writer_condvar.notify_all();
        collector_condvar.notify_all();
    }

    void scheduleFormatterThreadForUnitWithNumber(size_t ticket_number)
    {
        pool.scheduleOrThrowOnError([this, ticket_number] { formatterThreadFunction(ticket_number); });
    }

    void waitForFormattingFinished()
    {
        ///FIXME
        while(hasChunksToWorkWith())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    bool hasChunksToWorkWith()
    {
        return writer_unit_number - collector_unit_number > 0;
    }

    void collectorThreadFunction()
    {
        setThreadName("Collector");

        try
        {
            while (!formatting_finished)
            {
                const auto current_unit_number = collector_unit_number % processing_units.size();

                auto & unit = processing_units[current_unit_number];

                {
                    std::unique_lock<std::mutex> lock(mutex);
                    collector_condvar.wait(lock,
                        [&]{ return unit.status == READY_TO_READ; });
                }

                if (unit.type == ProcessingUnitType::FINALIZE)
                    break;

                if (unit.type == ProcessingUnitType::TOTALS) {
                   
                }

                assert(unit.status == READY_TO_READ);
                assert(unit.segment.size() > 0);

                /// Do main work here.
                out.write(unit.segment.data(), unit.actual_memory_size);

                if (need_flush.exchange(false))
                    IOutputFormat::flush();

                ++collector_unit_number;

                std::lock_guard<std::mutex> lock(mutex);
                unit.status = READY_TO_INSERT;
                writer_condvar.notify_all();
            }
        }
        catch (...)
        {
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

            /// TODO: Implement proper nextImpl
            BufferWithOutsideMemory<WriteBuffer> out_buffer(unit.segment);

            auto formatter = internal_formatter_creator(out_buffer);

            unit.actual_memory_size = 0;

            switch (unit.type) 
            {
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
                    break;
                }
            }
            
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
