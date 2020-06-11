#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <Common/Arena.h>
#include <IO/WriteBufferFromArena.h>

#include <deque>
#include <Common/setThreadName.h>

#include <atomic>

namespace DB
{

const size_t min_chunk_bytes_for_parallel_formatting = 1024;

class ParallelFormattingOutputFormat : public IOutputFormat
{
public:

    struct Params
    {
        const Block & header;
        WriteBuffer & out;
    };

    explicit ParallelFormattingOutputFormat(Params params)
        : IOutputFormat(params.header, params.out)
        , original_write_buffer(params.out)
    {

    }

    String getName() const override final { return "ParallelFormattingOutputFormat"; }

protected:
    void consume(Chunk chunk) override final
    {
        if (chunk.empty())
            formatting_finished = true;

        const auto current_unit_number = writer_unit_number % processing_units.size();

        auto & unit = processing_units[current_unit_number];

        {
            std::unique_lock<std::mutex> lock(mutex);
            writer_condvar.wait(lock,
                [&]{ return unit.status == READY_TO_INSERT || formatting_finished; });
        }

        assert(unit.status == READY_TO_INSERT);

        unit.chunk = std::move(chunk);
        /// TODO: Allocate new arena for current chunk


        /// TODO: Submit task to ThreadPool
    }

private:

    WriteBuffer & original_write_buffer;

    enum ProcessingUnitStatus
    {
        READY_TO_INSERT,
        READY_TO_FORMAT,
        READY_TO_READ
    };

    struct ProcessingUnit
    {
        Chunk chunk;
        Arena arena;
        ProcessingUnitStatus status;
    };



    std::deque<ProcessingUnit> processing_units;

    std::mutex mutex;
    std::atomic_bool formatting_finished{false};


    std::atomic_size_t collector_unit_number{0};
    std::atomic_size_t writer_unit_number{0};

    std::condition_variable collector_condvar;
    std::condition_variable writer_condvar;

    void сollectorThreadFunction()
    {
        setThreadName("Collector");

        while (!formatting_finished)
        {
            const auto current_unit_number = collector_unit_number % processing_units.size();

            auto & unit = processing_units[current_unit_number];

            {
                std::unique_lock<std::mutex> lock(mutex);
                collector_condvar.wait(lock,
                    [&]{ return unit.status == READY_TO_READ || formatting_finished; });
            }

            if (formatting_finished)
            {
                break;
            }

            assert(unit.status == READY_TO_READ);

            /// TODO: Arena is singly linked list, and it is simply a list of chunks.
            /// We have to write them all into original WriteBuffer.
            char * arena_begin = nullptr;
            size_t arena_size = 0;
            /// Do main work here.
            original_write_buffer.write(arena_begin, arena_size);

            /// How to drop this arena?


            std::lock_guard<std::mutex> lock(mutex);
            unit.status = READY_TO_INSERT;
            writer_condvar.notify_all();
        }

    }


    void formatterThreadFunction(size_t current_unit_number)
    {
        setThreadName("Formatter");

        auto & unit = processing_units[current_unit_number];

        const char * arena_begin = nullptr;
        WriteBufferFromArena out(unit.arena, arena_begin);

        /// TODO: create parser and parse current chunk to arena
    }
};

}
