#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include "Common/ArenaWithFreeLists.h"
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

    void initializeWorkers();

    class Segment
    {
    public:
        Segment(size_t size_, SynchronizedArenaWithFreeLists * arena_) : arena(arena_), m_data(arena->alloc(size_)), m_size(size_) { }

        Segment() = default;

        Segment(const Segment &) = delete;
        Segment & operator=(const Segment &) = delete;

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
    struct Range
    {
        size_t from;
        size_t to;
    };

    using ReaderWithRange = std::pair<ReadBufferPtr, Range>;

    class ReadBufferFactory
    {
    public:
        virtual std::optional<ReaderWithRange> getReader() = 0;
        virtual ~ReadBufferFactory() = default;
        virtual off_t seek(off_t off, int whence) = 0;
        virtual std::optional<size_t> getTotalSize() = 0;
    };

    explicit ParallelReadBuffer(std::unique_ptr<ReadBufferFactory> reader_factory_, size_t max_working_readers);

    ~ParallelReadBuffer() override { finishAndWait(); }

    off_t seek(off_t off, int whence) override;
    std::optional<size_t> getTotalSize() override;
    off_t getPosition() override;

private:
    /// Reader in progress with a list of read segments
    struct ReadWorker
    {
        explicit ReadWorker(ReadBufferPtr reader_, const Range & range_) : reader(reader_), range(range_) { }

        Segment nextSegment()
        {
            assert(!segments.empty());
            auto next_segment = std::move(segments.front());
            segments.pop_front();
            range.from += next_segment.size();
            return next_segment;
        }

        ReadBufferPtr reader;
        std::deque<Segment> segments;
        bool finished{false};
        Range range;
    };

    using ReadWorkerPtr = std::shared_ptr<ReadWorker>;

    /// First worker in deque have new data or processed all available amount
    inline bool currentWorkerReady() const
    {
        return !read_workers.empty() && (read_workers.front()->finished || !read_workers.front()->segments.empty());
    }

    /// First worker in deque processed and flushed all data
    inline bool currentWorkerCompleted() const
    {
        return !read_workers.empty() && read_workers.front()->finished && read_workers.front()->segments.empty();
    }

    /// Create new readers in a loop and process it with readerThreadFunction.
    void processor();

    /// Process read_worker, read data and save into internal segments queue
    void readerThreadFunction(ReadWorkerPtr read_worker);

    void onBackgroundException();
    void finishAndWait();

    SynchronizedArenaWithFreeLists arena;

    Segment current_segment;

    ThreadPool pool;

    std::unique_ptr<ReadBufferFactory> reader_factory;

    /**
     * FIFO queue of readers.
     * Each worker contains reader itself and downloaded segments.
     * When reader read all available data it will be removed from
     * deque and data from next reader will be consumed to user.
     */
    std::deque<ReadWorkerPtr> read_workers;
    std::condition_variable reader_condvar;

    std::mutex mutex;
    /// Triggered when new data available
    std::condition_variable next_condvar;

    std::exception_ptr background_exception = nullptr;
    std::atomic_bool emergency_stop{false};

    off_t current_position{0};

    bool all_completed{false};
    bool all_created{false};
};

}
