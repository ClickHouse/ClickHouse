
#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/SplittableBzip2ReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/threadPoolCallbackRunner.h>

namespace DB
{

class ParallelBzip2ReadBuffer : public CompressedReadBufferWrapper
{
private:
    bool nextImpl() override;

public:
    ParallelBzip2ReadBuffer(
        std::unique_ptr<SeekableReadBuffer> input_,
        ThreadPoolCallbackRunner<void> schedule_,
        size_t max_working_readers_,
        size_t range_step_,
        size_t file_size_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    ~ParallelBzip2ReadBuffer() override { finishAndWait(); }

private:
    /// Reader in progress with a buffer for the segment
    using ReadWorker = ParallelReadBuffer::ReadWorker;
    struct CompressedReadWorker : public ReadWorker
    {
        CompressedReadWorker(SeekableReadBuffer & input_, size_t offset, size_t size) : ReadWorker(input_, offset, size) { }

        bool hasUncompressedBytesToConsume() const { return uncompressed && uncompressed_bytes_produced > uncompressed_bytes_consumed; }

        std::unique_ptr<ReadBufferFromMemory> compressed;
        std::unique_ptr<SplittableBzip2ReadBuffer> decompressor;
        std::unique_ptr<WriteBufferFromString> uncompressed;
        std::string uncompressed_segment;
        size_t uncompressed_bytes_produced = 0;
        size_t uncompressed_bytes_consumed = 0;
    };

    using CompressedReadWorkerPtr = std::shared_ptr<CompressedReadWorker>;

    [[noreturn]] void handleEmergencyStop();

    void addReaders();
    bool addReaderToPool();

    /// Process read_worker, read data and save into the buffer
    void readerThreadFunction(CompressedReadWorkerPtr read_worker);

    void onBackgroundException();
    void finishAndWait();

    size_t max_working_readers;
    std::atomic_size_t active_working_readers{0};

    ThreadPoolCallbackRunner<void> schedule;

    SeekableReadBuffer & input;
    size_t file_size;
    size_t range_step;
    size_t buf_size;
    size_t next_range_start{0};

    /**
     * FIFO queue of readers.
     * Each worker contains a buffer for the downloaded segment.
     * After all data for the segment is read, uncompressed and delivered to the user, the reader will be removed
     * from deque and data from next reader will be delivered.
     * After removing from deque, call addReaders().
     */
    std::deque<CompressedReadWorkerPtr> compressed_read_workers;

    /// Triggered when new uncompressed data available
    std::condition_variable next_condvar;

    std::mutex exception_mutex;
    std::exception_ptr background_exception = nullptr;
    std::atomic_bool emergency_stop{false};

    bool all_completed{false};
};
}
