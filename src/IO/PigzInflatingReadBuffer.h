#pragma once

#include <IO/ReadBuffer.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/threadPoolCallbackRunner.h>

#include <zlib.h>

#include <future>
#include <list>
#include <memory>
#include <string>
#include <vector>


namespace DB
{

/// Performs gzip decompression of a stream produced by PigzDeflatingWriteBuffer, decompressing the
/// independent full-flush segments in parallel on the shared IO thread pool.
class PigzInflatingReadBuffer : public CompressedReadBufferWrapper
{
public:
    explicit PigzInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~PigzInflatingReadBuffer() override;

private:
    struct CompressedBuf
    {
        std::shared_ptr<Memory<>> mem;
        size_t len = 0;
        int rc_inflate = 0;
    };

    bool nextImpl() override;

    CompressedBuf decompressBlock(unsigned char * in_buf, size_t in_len);
    bool writeToInternal();
    void scheduleDecompressBlock(unsigned char * in_buf, size_t in_len);
    /// Await every scheduled decompression and move the results into `results`, preserving order.
    void appendScheduledResults();

    bool skipped_header_flag = false;
    bool eof_flag = false;

    std::list<std::shared_ptr<CompressedBuf>> results;
    std::list<std::shared_ptr<CompressedBuf>>::iterator curr_result_it;

    std::shared_ptr<Memory<>> working_memory;

    std::string prev_last_slice;

    /// Runs block decompression on the shared IO thread pool (created lazily on first use).
    ThreadPoolCallbackRunnerUnsafe<CompressedBuf> runner;
    std::vector<std::future<CompressedBuf>> block_futures;
};

}
