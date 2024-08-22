#include <Processors/Formats/Impl/ParallelFormattingOutputFormat.h>

#include <Common/setThreadName.h>
#include <Common/scope_guard_safe.h>

namespace DB
{
    void ParallelFormattingOutputFormat::finalizeImpl()
    {
        need_flush = true;
        /// Don't throw any background_exception here, because we want to finalize the execution.
        /// Exception will be checked after main thread is finished.
        addChunk(Chunk{}, ProcessingUnitType::FINALIZE, /*can_throw_exception*/ false);
        collector_finished.wait();

        {
            std::lock_guard lock(collector_thread_mutex);
            if (collector_thread.joinable())
                collector_thread.join();
        }

        {
            std::lock_guard lock(mutex);

            if (background_exception)
            {
                collector_finished.set();
                rethrowBackgroundException();
            }
        }

        /// The code below is required to write valid output in case of exception during parallel parsing,
        /// because we finish formatting and collecting threads in case of exception.
        /// So, in case of exception after finalize we could still not output prefix/suffix or finalize underlying format.

        if (collected_prefix && collected_suffix && collected_finalize)
            return;

        auto formatter = internal_formatter_creator(out);
        formatter->setRowsReadBefore(rows_collected);
        formatter->setException(exception_message);

        if (!collected_prefix && (need_write_prefix || started_prefix))
            formatter->writePrefix();

        if (!collected_suffix && (need_write_suffix || started_suffix))
            formatter->writeSuffix();

        if (!collected_finalize)
            formatter->finalizeImpl();

        formatter->finalizeBuffers();
    }

    void ParallelFormattingOutputFormat::addChunk(Chunk chunk, ProcessingUnitType type, bool can_throw_exception)
    {
        {
            std::lock_guard lock(mutex);
            if (background_exception && can_throw_exception)
                rethrowBackgroundException();
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
        if (type == ProcessingUnitType::FINALIZE)
        {
            std::lock_guard lock(statistics_mutex);
            unit.statistics = std::move(statistics);
        }

        size_t first_row_num = rows_consumed;
        if (unit.type == ProcessingUnitType::PLAIN)
        {
            rows_consumed += unit.chunk.getNumRows();
            unit.rows_num = unit.chunk.getNumRows();
        }

        scheduleFormatterThreadForUnitWithNumber(current_unit_number, first_row_num);
        ++writer_unit_number;
    }


    void ParallelFormattingOutputFormat::finishAndWait() noexcept
    {
        emergency_stop = true;

        {
            std::unique_lock<std::mutex> lock(mutex);
            collector_condvar.notify_all();
            writer_condvar.notify_all();
        }

        {
            std::lock_guard lock(collector_thread_mutex);
            if (collector_thread.joinable())
                collector_thread.join();
        }

        try
        {
            pool.wait();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


    void ParallelFormattingOutputFormat::collectorThreadFunction(const ThreadGroupPtr & thread_group)
    {
        SCOPE_EXIT_SAFE(
            if (thread_group)
                CurrentThread::detachFromGroupIfNotDetached();
        );
        setThreadName("Collector");
        if (thread_group)
            CurrentThread::attachToGroupIfDetached(thread_group);

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
                auto copy_of_unit_type = unit.type;

                /// Do main work here.
                out.write(unit.segment.data(), unit.actual_memory_size);

                if (need_flush.exchange(false) || auto_flush)
                    IOutputFormat::flush();

                ++collector_unit_number;
                rows_collected += unit.rows_num;

                {
                    /// Notify other threads.
                    std::lock_guard lock(mutex);
                    unit.status = READY_TO_INSERT;
                    writer_condvar.notify_all();
                }

                if (copy_of_unit_type == ProcessingUnitType::START)
                {
                    collected_prefix = true;
                }
                else if (copy_of_unit_type == ProcessingUnitType::PLAIN_FINISH)
                {
                    collected_suffix = true;
                }
                /// We can exit only after writing last piece of data to out buffer.
                else if (copy_of_unit_type == ProcessingUnitType::FINALIZE)
                {
                    collected_finalize = true;
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

    void ParallelFormattingOutputFormat::formatterThreadFunction(size_t current_unit_number, size_t first_row_num, const ThreadGroupPtr & thread_group)
    {
        SCOPE_EXIT_SAFE(
            if (thread_group)
                CurrentThread::detachFromGroupIfNotDetached();
        );
        setThreadName("Formatter");
        if (thread_group)
            CurrentThread::attachToGroupIfDetached(thread_group);

        try
        {
            auto & unit = processing_units[current_unit_number];
            assert(unit.status == READY_TO_FORMAT);

            /// We want to preallocate memory buffer (increase capacity)
            /// and put the pointer at the beginning of the buffer
            unit.segment.resize(DBMS_DEFAULT_BUFFER_SIZE);

            unit.actual_memory_size = 0;
            BufferWithOutsideMemory<WriteBuffer> out_buffer(unit.segment);

            /// The second invocation won't release memory, only set size equals to 0.
            unit.segment.resize(0);

            auto formatter = internal_formatter_creator(out_buffer);
            formatter->setRowsReadBefore(first_row_num);
            formatter->setException(exception_message);

            switch (unit.type)
            {
                case ProcessingUnitType::START:
                {
                    formatter->writePrefix();
                    break;
                }
                case ProcessingUnitType::PLAIN:
                {
                    formatter->consume(std::move(unit.chunk));
                    break;
                }
                case ProcessingUnitType::PLAIN_FINISH:
                {
                    formatter->writeSuffix();
                    break;
                }
                case ProcessingUnitType::TOTALS:
                {
                    formatter->consumeTotals(std::move(unit.chunk));
                    break;
                }
                case ProcessingUnitType::EXTREMES:
                {
                    if (are_totals_written)
                        formatter->setTotalsAreWritten();
                    formatter->consumeExtremes(std::move(unit.chunk));
                    break;
                }
                case ProcessingUnitType::FINALIZE:
                {
                    formatter->statistics = std::move(unit.statistics);
                    formatter->finalizeImpl();
                    break;
                }
            }

            /// Flush all the data to handmade buffer.
            formatter->flush();
            formatter->finalizeBuffers();
            out_buffer.finalize();
            unit.actual_memory_size = out_buffer.getActualSize();

            {
                std::lock_guard lock(mutex);
                unit.status = READY_TO_READ;
                collector_condvar.notify_all();
            }
        }
        catch (...)
        {
            onBackgroundException();
        }
    }
}
