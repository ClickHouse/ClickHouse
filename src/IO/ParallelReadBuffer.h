#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ArenaWithFreeLists.h>

namespace DB
{

/**
 * Reads from multiple positions in a ReadBuffer in parallel.
 * Then reassembles the data into one stream in the original order.
 *
 * Each working reader reads its segment of data into a buffer.
 *
 * ParallelReadBuffer in nextImpl method take first available segment from first reader in deque and reports it it to user.
 * When first reader finishes reading, they will be removed from worker deque and data from next reader consumed.
 *
 * Number of working readers limited by max_working_readers.
 */
class ParallelReadBuffer : public SeekableReadBuffer, public WithFileSize
{
private:
    /// Blocks until data occurred in the first reader or this reader indicate finishing
    /// Finished readers removed from queue and data from next readers processed
    bool nextImpl() override;

public:
    ParallelReadBuffer(SeekableReadBuffer & input, ThreadPoolCallbackRunnerUnsafe<void> schedule_, size_t max_working_readers, size_t range_step_, size_t file_size);

    ~ParallelReadBuffer() override { finishAndWait(); }

    off_t seek(off_t off, int whence) override;
    std::optional<size_t> tryGetFileSize() override;
    off_t getPosition() override;

    const SeekableReadBuffer & getReadBuffer() const { return input; }
    SeekableReadBuffer & getReadBuffer() { return input; }

private:
    /// Reader in progress with a buffer for the segment
    struct ReadWorker;
    using ReadWorkerPtr = std::shared_ptr<ReadWorker>;

    /// First worker in deque have new data or processed all available amount
    bool currentWorkerReady() const;
    /// First worker in deque processed and flushed all data
    bool currentWorkerCompleted() const;

    [[noreturn]] void handleEmergencyStop();

    void addReaders();
    bool addReaderToPool();

    /// Process read_worker, read data and save into the buffer
    void readerThreadFunction(ReadWorkerPtr read_worker);

    void onBackgroundException();
    void finishAndWait();

    size_t max_working_readers;
    std::atomic_size_t active_working_readers{0};

    ThreadPoolCallbackRunnerUnsafe<void> schedule;

    SeekableReadBuffer & input;
    size_t file_size;
    size_t range_step;
    size_t next_range_start{0};

    /**
     * FIFO queue of readers.
     * Each worker contains a buffer for the downloaded segment.
     * After all data for the segment is read and delivered to the user, the reader will be removed
     * from deque and data from next reader will be delivered.
     * After removing from deque, call addReaders().
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

/// If `buf` is a SeekableReadBuffer with supportsReadAt() == true, creates a ParallelReadBuffer
/// from it. Otherwise returns nullptr;
std::unique_ptr<ParallelReadBuffer> wrapInParallelReadBufferIfSupported(
    ReadBuffer & buf, ThreadPoolCallbackRunnerUnsafe<void> schedule, size_t max_working_readers,
    size_t range_step, size_t file_size);

}
