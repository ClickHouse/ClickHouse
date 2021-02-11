#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/ThreadPool.h>

namespace DB
{

/**
 * Reads from multiple ReadBuffers in parallel.
 * Preserves order of readers obtained from ReadBufferFactory.
 *
 * It consumes multiple readers and yields data from them in order as it passed.
 *
 * The main idea is similar to ParallelParsingInputFormat/ParallelFormattingOutputFormat.
 * There are fixed number of processing unit, each of them can be in one of possible states:
 * waiting for data from reader, filled with data, stopped (see ProcessingUnitStatus).
 *
 * Note that child buffers (obtained from ReadBufferFactory) read until the end in,
 * so it should be small (not grater than `buffer_size`).
 * Data from up to `max_working_readers` readers can be loaded into memory simultaneously.
 *
 */
class ParallelReadBuffer : public ReadBuffer
{
private:

    /// Blocks until data occurred in the first reader or this reader indicate finishing
    /// Finished readers removed from queue and data from next readers processed
    bool nextImpl() override;

public:

    class ReadBufferFactory
    {
    public:
        virtual ReadBufferPtr getReader() = 0;
        virtual ~ReadBufferFactory() = default;
    };

    explicit ParallelReadBuffer(
        std::unique_ptr<ReadBufferFactory> reader_factory_, size_t max_working_readers, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
        : ReadBuffer(nullptr, 0)
        , pool(max_working_readers + 1)
        , reader_factory(std::move(reader_factory_))
    {
        processing_units.reserve(max_working_readers);
        std::lock_guard<std::mutex> lock(mutex);
        for (size_t i = 0; i < max_working_readers; ++i)
            processing_units.emplace_back(buffer_size);
        pool.scheduleOrThrow([this]() { readerThreadFunction(); });
    }

    ~ParallelReadBuffer() override { finishAndWait(); }

private:

    /// Processing unit lifetime:
    /// - initial: READY_TO_INSERT
    /// - started to fill unit's buffer: READY_TO_INSERT -> READING
    /// - buffer filled with data: READING -> READY_TO_READ
    /// - data copied into working_buffer: READY_TO_READ -> READY_TO_INSERT
    /// - when buffer READY_TO_INSERT but no more readers: READY_TO_INSERT -> STOPPED
    enum ProcessingUnitStatus
    {
        /// Reader can fill unit's buffer with data
        READY_TO_INSERT,
        /// Reading into buffer in progress
        READING,
        /// Data ready to be provided to user
        READY_TO_READ,
        /// Buffer not filled and no more readers to obtain data
        STOPPED
    };

    struct ProcessingUnit
    {
        explicit ProcessingUnit(size_t buffer_size)
            : status(ProcessingUnitStatus::READY_TO_INSERT)
            , segment(buffer_size)
        {
        }

        ProcessingUnitStatus status;
        Memory<> segment;
        ReadBufferPtr reader;
    };

    void readerThreadFunction();
    void readerUnitThreadFunction(size_t unit_number);

    void onBackgroundException();
    void finishAndWait();

    ThreadPool pool;

    Memory<> segment;
    std::unique_ptr<ReadBufferFactory> reader_factory;

    /// Number of unit that used to write data to
    std::atomic<size_t> current_write_unit{0};
    /// Number of unit data from which data should be read
    std::atomic<size_t> current_read_unit{0};
    std::vector<ProcessingUnit> processing_units;

    std::mutex mutex;
    /// triggered when new data available
    std::condition_variable next_condvar;
    /// triggered when some data consumed and reader can fill next unit
    std::condition_variable reader_condvar;

    std::exception_ptr background_exception = nullptr;
    std::atomic_bool emergency_stop{false};

    std::atomic_bool no_more_readers{false};
    bool all_done{false};
};


}
