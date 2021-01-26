#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/ThreadPool.h>

namespace DB
{

/**
 * Reads from multiple ReadBuffers in parallel.
 *
 * It consumes multiple readers and yields data from them in order as it passed.
 * Each working reader can read up to max_segments_per_worker chunks into temporary buffers.
 * Number of working readers limited by max_working_readers.
 */
class ReadBufferFanIn : public ReadBuffer
{
private:

    /// Blocks until data occurred in the first reader or this reader indicate finishing
    /// Finished readers removed from queue and data from next readers processed
    bool nextImpl() override;

public:

    ReadBufferFanIn(size_t max_working_readers_, size_t max_segments_per_worker_ = 10)
    : ReadBuffer(nullptr, 0)
    , max_working_readers(max_working_readers_)
    , max_segments_per_worker(max_segments_per_worker_)
    {
        pool.setMaxThreads(max_working_readers);
    }

    ~ReadBufferFanIn() override
    {
        finishAndWait();
    }

    /// Adds reader to queue
    /// Mustn't be called after start
    void addReader(ReadBufferPtr reader);

    /// Indicates that all readers passed to addReader
    void start();

private:

    struct ProcessingUnit
    {
        explicit ProcessingUnit(ReadBufferPtr reader_, size_t number_)
            : reader(reader_)
            , number(number_)
        {}

        ReadBufferPtr reader;
        std::deque<Memory<>> segments;
        bool executing{false};
        bool finished{false};

        const size_t number;
    };
    using ProcessingUnitPtr = std::shared_ptr<ProcessingUnit>;

    /// Read data from unit->reader and put in into unit->segments
    void readerThreadFunction(ProcessingUnitPtr unit);

    /// Choose first pending unit form queue
    ProcessingUnitPtr chooseNextReader();

    void onBackgroundException();
    void finishAndWait();

    Memory<> segment;

    const size_t max_working_readers;
    const size_t max_segments_per_worker;
    ThreadPool pool;

    /// FIFO queue of readers
    /// Each unit contains reader itself and up to max_working_readers read segments
    std::deque<ProcessingUnitPtr> readers;

    std::mutex mutex;
    /// triggered when new data available
    std::condition_variable next_condvar;

    /// triggered when some data consumed and reader can continue reading
    std::condition_variable reader_condvar;

    std::exception_ptr background_exception = nullptr;
    std::atomic_bool emergency_stop{false};

    bool started{false};
    bool all_done{false};

    std::atomic_size_t last_num{0};
};


}
