#pragma once

#include <IO/ReadBuffer.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>
#include <Common/ThreadPool.h>

#include <zlib.h>


namespace DB
{

/// Performs compression using zlib library, compress data in parallel and writes it to out_ WriteBuffer.
class PigzInflatingReadBuffer : public CompressedReadBufferWrapper
{
public:
    PigzInflatingReadBuffer(
        std::unique_ptr<ReadBuffer> in_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~PigzInflatingReadBuffer() override;

private:
    bool nextImpl() override;

    struct CompressedBuf {
        std::shared_ptr<Memory<>> mem;
        size_t len;
        int rc_inflate;
    };

    CompressedBuf decompressBlock(unsigned char * in_buf, size_t in_len);
    bool writeToInternal();

    bool skipped_header_flag = false;
    bool eof_flag = false;
    ThreadPool pool;

    size_t internal_pos = 0;

    std::vector<CompressedBuf> results;
    size_t curr_result_i = 0;
    size_t curr_result_pos = 0;

    std::string prev_last_slice;

    size_t sum_decomp = 0;
    size_t sum_written = 0;
};

}
