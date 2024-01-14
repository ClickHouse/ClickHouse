#include "ParallelBzip2ReadBuffer.h"
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/SharedThreadPools.h>
#include <IO/SplittableBzip2ReadBuffer.h>
#include <IO/copyData.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNEXPECTED_END_OF_FILE;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int SEEK_POSITION_OUT_OF_BOUND;
extern const int LOGICAL_ERROR;

}

ParallelBzip2ReadBuffer::ParallelBzip2ReadBuffer(
    std::unique_ptr<SeekableReadBuffer> input_,
    ThreadPoolCallbackRunner<void> schedule_,
    size_t max_working_readers_,
    size_t range_step_,
    size_t file_size_,
    size_t buf_size_)
    : CompressedReadBufferWrapper(std::move(input_), 0, nullptr, 0)
    , max_working_readers(max_working_readers_)
    , schedule(std::move(schedule_))
    , input(dynamic_cast<SeekableReadBuffer &>(getWrappedReadBuffer()))
    , file_size(file_size_)
    , range_step(std::max(1ul, range_step_))
    , buf_size(buf_size_)
{
    LOG_TRACE(&Poco::Logger::get("ParallelBzip2ReadBuffer"), "Parallel bzip2 reading is used");
    try
    {
        addReaders();
    }
    catch (const Exception &)
    {
        finishAndWait();
        throw;
    }
}

bool ParallelBzip2ReadBuffer::addReaderToPool()
{
    if (next_range_start >= file_size)
        return false;

    size_t range_start = next_range_start;
    size_t range_end = std::min(range_start + range_step, file_size);
    bool last_split = (range_end == file_size);
    LOG_TRACE(&Poco::Logger::get("ParallelBzip2ReadBuffer"), "Add reader for range: [{}, {}) before adjustment", range_start, range_end);

    /// Adjust range_end according to the next block delimiter after current split.
    if (!last_split)
    {
        Int64 bs_buff = 0;
        Int64 bs_live = 0;
        input.seek(range_end, SEEK_SET);
        bool ok = SplittableBzip2ReadBuffer::skipToNextMarker(
            SplittableBzip2ReadBuffer::BLOCK_DELIMITER, SplittableBzip2ReadBuffer::DELIMITER_BIT_LENGTH, input, bs_buff, bs_live);
        if (ok)
        {
            range_end = input.getPosition() - SplittableBzip2ReadBuffer::DELIMITER_BIT_LENGTH / 8 + 1;
            next_range_start = range_start + range_step;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find next block delimiter in after offset: {}", range_end);
    }
    else
    {
        next_range_start = file_size;
    }
    LOG_TRACE(&Poco::Logger::get("ParallelBzip2ReadBuffer"), "Add reader for range: [{}, {}) after adjustment", range_start, range_end);

    auto worker = compressed_read_workers.emplace_back(std::make_shared<CompressedReadWorker>(input, range_start, range_end - range_start));
    ++active_working_readers;
    schedule([this, my_worker = std::move(worker)]() mutable { readerThreadFunction(std::move(my_worker)); }, Priority{});
    return true;
}

void ParallelBzip2ReadBuffer::addReaders()
{
    while (compressed_read_workers.size() < max_working_readers && addReaderToPool())
        ;
}

void ParallelBzip2ReadBuffer::handleEmergencyStop()
{
    // this can only be called from the main thread when there is an exception
    assert(background_exception);
    std::rethrow_exception(background_exception);
}

bool ParallelBzip2ReadBuffer::nextImpl()
{
    while (true)
    {
        /// All readers processed, stop
        if (compressed_read_workers.empty())
        {
            chassert(next_range_start >= file_size);
            return false;
        }

        auto * w = compressed_read_workers.front().get();

        std::unique_lock lock{w->worker_mutex};

        if (emergency_stop)
            handleEmergencyStop(); // throws

        /// Read uncompressed data from front reader
        if (w->hasUncompressedBytesToConsume())
        {
            Position begin = w->uncompressed_segment.data() + w->uncompressed_bytes_consumed;
            size_t length = std::min(w->uncompressed_bytes_produced - w->uncompressed_bytes_consumed, buf_size);
            working_buffer = internal_buffer = Buffer(begin, begin + length);
            w->uncompressed_bytes_consumed += length;
            LOG_TRACE(
                &Poco::Logger::get("ParallelBzip2ReadBuffer"),
                "Read {}/{} bytes from reader with range starting from {}",
                w->uncompressed_bytes_consumed,
                w->uncompressed_bytes_produced,
                w->start_offset);
            return true;
        }

        /// All uncompressed data is read from front reader, remove it
        if (w->uncompressed)
        {
            lock.unlock();
            compressed_read_workers.pop_front();
            addReaders();
            continue;
        }

        /// Waiting for uncompressed data
        next_condvar.wait_for(lock, std::chrono::seconds(10));
    }

    chassert(false);
    return false;
}

void ParallelBzip2ReadBuffer::readerThreadFunction(CompressedReadWorkerPtr worker)
{
    SCOPE_EXIT({
        if (active_working_readers.fetch_sub(1) == 1)
            active_working_readers.notify_all();
    });

    try
    {
        auto on_progress = [&](size_t bytes_read) -> bool
        {
            if (emergency_stop || worker->cancel)
                return true;

            std::lock_guard lock(worker->worker_mutex);

            /// No progress, should continue read
            if (bytes_read <= worker->bytes_produced)
                return false;
            worker->bytes_produced = bytes_read;

            /// If (compressed) segment is all read, then decompress it into uncompressed_segment
            if (!worker->hasBytesToProduce())
            {
                Stopwatch watch;
                worker->compressed = std::make_unique<ReadBufferFromMemory>(worker->segment.data(), worker->segment.size());
                worker->decompressor = std::make_unique<SplittableBzip2ReadBuffer>(std::move(worker->compressed), buf_size);
                worker->uncompressed_segment.reserve(worker->segment.size() * 9);
                worker->uncompressed = std::make_unique<WriteBufferFromString>(worker->uncompressed_segment);
                copyData(*worker->decompressor, *worker->uncompressed);
                worker->uncompressed_segment.resize(worker->uncompressed->count());
                LOG_TRACE(
                    &Poco::Logger::get("ParallelBzip2ReadBuffer"),
                    "Read and decompressed range: [{}, {}) in {} seconds, compressed size: {}, uncompressed size: {}",
                    worker->start_offset,
                    worker->start_offset + worker->segment.size(),
                    watch.elapsedSeconds(),
                    worker->segment.size(),
                    worker->uncompressed_segment.size());


                worker->bytes_consumed = worker->bytes_produced;
                worker->uncompressed_bytes_consumed = 0;
                worker->uncompressed_bytes_produced = worker->uncompressed_segment.size();
                next_condvar.notify_all();

                /// Free memory in advance to reduce peak memory
                worker->decompressor.reset();
                Memory<>().swap(worker->segment);
            }

            return false;
        };

        size_t r = input.readBigAt(worker->segment.data(), worker->segment.size(), worker->start_offset, on_progress);

        if (!on_progress(r) && r < worker->segment.size())
            throw Exception(
                ErrorCodes::UNEXPECTED_END_OF_FILE,
                "Failed to read all the data from the reader at offset {}, got {}/{} bytes",
                worker->start_offset,
                r,
                worker->segment.size());
    }
    catch (...)
    {
        onBackgroundException();
    }
}

void ParallelBzip2ReadBuffer::onBackgroundException()
{
    std::lock_guard lock{exception_mutex};
    if (!background_exception)
        background_exception = std::current_exception();

    emergency_stop = true;
    next_condvar.notify_all();
}

void ParallelBzip2ReadBuffer::finishAndWait()
{
    emergency_stop = true;

    size_t active_readers = active_working_readers.load();
    while (active_readers != 0)
    {
        active_working_readers.wait(active_readers);
        active_readers = active_working_readers.load();
    }
}
}
