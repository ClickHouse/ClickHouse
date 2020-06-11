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

const size_t min_chunk_bytes_for_parallel_formatting_ = 1024;
const size_t max_threads_for_parallel_formatting_ = 4;

class ParallelFormattingOutputFormat : public IOutputFormat
{
public:
    /* Used to recreate formatter on every new data piece. */
    using InternalFormatterCreator = std::function<OutputFormatPtr(WriteBuffer & buf)>;

    struct Params
    {
        WriteBuffer & out_;
        const Block & header_;
        InternalFormatterCreator internal_formatter_creator_;
    };

    explicit ParallelFormattingOutputFormat(Params params)
        : IOutputFormat(params.header_, params.out_)
        , internal_formatter_creator(params.internal_formatter_creator_)
        , pool(max_threads_for_parallel_formatting_)

    {
//        std::cout << "ParallelFormattingOutputFormat::constructor" << std::endl;
        processing_units.resize(max_threads_for_parallel_formatting_ + 2);

        collector_thread = ThreadFromGlobalPool([this] { сollectorThreadFunction(); });
    }

    ~ParallelFormattingOutputFormat() override
    {
        finishAndWait();
    }

    String getName() const override final { return "ParallelFormattingOutputFormat"; }

protected:
    void consume(Chunk chunk) override final
    {

        std::cout << StackTrace().toString() << std::endl;

        if (chunk.empty())
        {
//            std::cout << "Empty chunk" << std::endl;
            formatting_finished = true;
        }


        const auto current_unit_number = writer_unit_number % processing_units.size();

        auto & unit = processing_units[current_unit_number];

//        std::cout << "Consume " << current_unit_number << " before wait" << std::endl;

        {
            std::unique_lock<std::mutex> lock(mutex);
            writer_condvar.wait(lock,
                [&]{ return unit.status == READY_TO_INSERT || formatting_finished; });
        }

//        std::cout << "Consume " << current_unit_number << " after wait" << std::endl;

        assert(unit.status == READY_TO_INSERT);

        unit.chunk = std::move(chunk);

        /// Resize memory without deallocate
        unit.segment.resize(0);
        unit.status = READY_TO_FORMAT;

        scheduleFormatterThreadForUnitWithNumber(current_unit_number);

        ++writer_unit_number;
    }

private:

    InternalFormatterCreator internal_formatter_creator;

    enum ProcessingUnitStatus
    {
        READY_TO_INSERT,
        READY_TO_FORMAT,
        READY_TO_READ
    };

    struct ProcessingUnit
    {
        explicit ProcessingUnit()
            : status(ProcessingUnitStatus::READY_TO_INSERT)
        {
        }

        std::atomic<ProcessingUnitStatus> status;
        Chunk chunk;
        Memory<> segment;
    };

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

        std::cout << "finishAndWait()" << std::endl;
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
//        std::cout << "onBackgroundException" << std::endl;
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

    void сollectorThreadFunction()
    {
        setThreadName("Collector");

        try
        {
            while (!formatting_finished)
            {
                const auto current_unit_number = collector_unit_number % processing_units.size();

                auto &unit = processing_units[current_unit_number];

//                std::cout << "Collector " << current_unit_number << " before wait" << std::endl;

                {
                    std::unique_lock<std::mutex> lock(mutex);
                    collector_condvar.wait(lock,
                        [&]{ return unit.status == READY_TO_READ || formatting_finished; });
                }

//                std::cout << "Collector " << current_unit_number << " after wait" << std::endl;

                if (formatting_finished)
                {
//                    std::cout << "formatting finished" << std::endl;
                    break;
                }

                assert(unit.status == READY_TO_READ);
                assert(unit.segment.size() > 0);

                for (size_t i=0; i < unit.segment.size(); ++i)
                {
                    std::cout << *(unit.segment.data() + i);
                }
                std::cout << std::endl;


                /// Do main work here.
                out.write(unit.segment.data(), unit.segment.size());

                flush();

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
//            std::cout << "Formatter " << current_unit_number << std::endl;

            auto & unit = processing_units[current_unit_number];

            assert(unit.status = READY_TO_FORMAT);

            unit.segment.resize(10 * DBMS_DEFAULT_BUFFER_SIZE);

            /// TODO: Implement proper nextImpl
            BufferWithOutsideMemory<WriteBuffer> out(unit.segment);

            auto formatter = internal_formatter_creator(out);

            formatter->consume(std::move(unit.chunk));

//            std::cout << "Formatter " << current_unit_number << " before notify" << std::endl;

            {
                std::lock_guard<std::mutex> lock(mutex);
                unit.status = READY_TO_READ;
                collector_condvar.notify_all();
            }

//            std::cout << "Formatter " << current_unit_number << " after notify" << std::endl;
        }
        catch (...)
        {
            onBackgroundException();
        }

    }
};

}
