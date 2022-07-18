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
class ParallelReadBuffer : public SeekableReadBufferWithSize
{
private:
    /// Blocks until data occurred in the first reader or this reader indicate finishing
    /// Finished readers removed from queue and data from next readers processed
    bool nextImpl() override;

    class Segment : private boost::noncopyable
    {
    public:
        Segment(size_t size_, SynchronizedArenaWithFreeLists * arena_) : arena(arena_), m_data(arena->alloc(size_)), m_size(size_) { }

        Segment() = default;

        Segment(Segment && other) noexcept : arena(other.arena)
        {
            std::swap(m_data, other.m_data);
            std::swap(m_size, other.m_size);
        }

        Segment & operator=(Segment && other) noexcept
        {
            arena = other.arena;
            std::swap(m_data, other.m_data);
            std::swap(m_size, other.m_size);
            return *this;
        }

        ~Segment()
        {
            if (m_data)
            {
                arena->free(m_data, m_size);
            }
        }

        auto data() const noexcept { return m_data; }
        auto size() const noexcept { return m_size; }

    private:
        SynchronizedArenaWithFreeLists * arena{nullptr};
        char * m_data{nullptr};
        size_t m_size{0};
    };

public:
    class ReadBufferFactory
    {
    public:
        virtual SeekableReadBufferPtr getReader() = 0;
        virtual ~ReadBufferFactory() = default;
        virtual off_t seek(off_t off, int whence) = 0;
        virtual std::optional<size_t> getTotalSize() = 0;
    };

    explicit ParallelReadBuffer(std::unique_ptr<ReadBufferFactory> reader_factory_, CallbackRunner schedule_, size_t max_working_readers);

    ~ParallelReadBuffer() override { finishAndWait(); }

    off_t seek(off_t off, int whence) override;
    std::optional<size_t> getTotalSize() override;
    off_t getPosition() override;

private:
    /// Reader in progress with a list of read segments
    struct ReadWorker
    {
        explicit ReadWorker(SeekableReadBufferPtr reader_) : reader(std::move(reader_)), range(reader->getRemainingReadRange())
        {
            assert(range.right);
            bytes_left = *range.right - range.left + 1;
        }

        Segment nextSegment()
        {
            assert(!segments.empty());
            auto next_segment = std::move(segments.front());
            segments.pop_front();
            range.left += next_segment.size();
            return next_segment;
        }

        SeekableReadBufferPtr reader;
        std::deque<Segment> segments;
        bool finished{false};
        SeekableReadBuffer::Range range;
        size_t bytes_left{0};
        std::atomic_bool cancel{false};
    };

    using ReadWorkerPtr = std::shared_ptr<ReadWorker>;

    /// First worker in deque have new data or processed all available amount
    bool currentWorkerReady() const;
    /// First worker in deque processed and flushed all data
    bool currentWorkerCompleted() const;

    void handleEmergencyStop();

    void addReaders(std::unique_lock<std::mutex> & buffer_lock);
    bool addReaderToPool(std::unique_lock<std::mutex> & buffer_lock);

    /// Process read_worker, read data and save into internal segments queue
    void readerThreadFunction(ReadWorkerPtr read_worker);

    void onBackgroundException();
    void finishAndWait();

    SynchronizedArenaWithFreeLists arena;

    Segment current_segment;

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

    std::mutex mutex;
    /// Triggered when new data available
    std::condition_variable next_condvar;

    std::exception_ptr background_exception = nullptr;
    std::atomic_bool emergency_stop{false};

    off_t current_position{0};

    bool all_completed{false};
};

}
