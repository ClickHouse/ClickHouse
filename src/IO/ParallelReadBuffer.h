#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Common/ArenaWithFreeLists.h>

namespace DB
{

/**
 * Reads from multiple ReadBuffers in parallel.
 * Preserves order of readers obtained from SeekableReadBufferFactory.
 *
 * It consumes multiple readers and yields data from them in order as it passed.
 * Each working reader save segments of data to internal queue.
 *
 * ParallelReadBuffer in nextImpl method take first available segment from first reader in deque and fed it to user.
 * When first reader finish reading, they will be removed from worker deque and data from next reader consumed.
 *
 * Number of working readers limited by max_working_readers.
 */
class ParallelReadBuffer : public SeekableReadBuffer
{
private:
    /// Blocks until data occurred in the first reader or this reader indicate finishing
    /// Finished readers removed from queue and data from next readers processed
    bool nextImpl() override;

public:
    ParallelReadBuffer(SeekableReadBufferFactoryPtr reader_factory_, ThreadPoolCallbackRunner<void> schedule_, size_t max_working_readers, size_t range_step_);

    ~ParallelReadBuffer() override { finishAndWait(); }

    off_t seek(off_t off, int whence) override;
    size_t getFileSize();
    off_t getPosition() override;

    const SeekableReadBufferFactory & getReadBufferFactory() const { return *reader_factory; }
    SeekableReadBufferFactory & getReadBufferFactory() { return *reader_factory; }

private:
    /// Reader in progress with a list of read segments
    struct ReadWorker;
    using ReadWorkerPtr = std::shared_ptr<ReadWorker>;

    /// First worker in deque have new data or processed all available amount
    bool currentWorkerReady() const;
    /// First worker in deque processed and flushed all data
    bool currentWorkerCompleted() const;

    [[noreturn]] void handleEmergencyStop();

    void addReaders();
    bool addReaderToPool();

    /// Process read_worker, read data and save into internal segments queue
    void readerThreadFunction(ReadWorkerPtr read_worker);

    void onBackgroundException();
    void finishAndWait();

    Memory<> current_segment;

    size_t max_working_readers;
    std::atomic_size_t active_working_reader{0};

    ThreadPoolCallbackRunner<void> schedule;

    std::unique_ptr<SeekableReadBufferFactory> reader_factory;
    size_t range_step;
    size_t next_range_start{0};

    /**
     * FIFO queue of readers.
     * Each worker contains reader itself and downloaded segments.
     * When reader read all available data it will be removed from
     * deque and data from next reader will be consumed to user.
     */
    std::deque<ReadWorkerPtr> read_workers;

    /// Triggered when new data available
    std::condition_variable next_condvar;

    std::mutex exception_mutex;
    std::exception_ptr background_exception = nullptr;
    std::atomic_bool emergency_stop{false};

    off_t current_position{0}; // end of working_buffer

    bool all_completed{false};
};

}
