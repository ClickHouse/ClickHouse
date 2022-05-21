#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/ThreadPool.h>

namespace DB
{

/**
 * Reads from multiple ReadBuffers in parallel.
 * Preserves order of readers obtained from ReadBufferFactory.
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
    class ReadBufferFactory : public WithFileSize
    {
    public:
        virtual SeekableReadBufferPtr getReader() = 0;
        virtual ~ReadBufferFactory() override = default;
        virtual off_t seek(off_t off, int whence) = 0;
    };

    explicit ParallelReadBuffer(std::unique_ptr<ReadBufferFactory> reader_factory_, CallbackRunner schedule_, size_t max_working_readers);

    ~ParallelReadBuffer() override { finishAndWait(); }

    off_t seek(off_t off, int whence) override;
    std::optional<size_t> getFileSize();
    off_t getPosition() override;

    const ReadBufferFactory & getReadBufferFactory() const { return *reader_factory; }

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

    CallbackRunner schedule;

    std::unique_ptr<ReadBufferFactory> reader_factory;

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

    off_t current_position{0};

    bool all_completed{false};
};

}
