#include <IO/PigzInflatingReadBuffer.h>
#include <IO/PigzDeflatingWriteBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <cstring>
#include <exception>
#include <iterator>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_INFLATE_FAILED;
}

PigzInflatingReadBuffer::PigzInflatingReadBuffer(
    std::unique_ptr<ReadBuffer> in_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
{
    curr_result_it = results.end();
}

PigzInflatingReadBuffer::~PigzInflatingReadBuffer()
{
    /// Make sure no scheduled task still references this buffer (e.g. if scheduling was interrupted).
    /// wait() is enough: when the future becomes ready the task has finished and released its captures.
    for (auto & future : block_futures)
    {
        if (future.valid())
            future.wait();
    }
}

bool PigzInflatingReadBuffer::nextImpl()
{
    /// Serve any already-decompressed block first.
    if (!writeToInternal())
        return true;

    if (eof_flag)
        return false;

    if (in->eof())
    {
        eof_flag = true;
        /// Decompress the trailing segment (the final, Z_FINISH block) carried over so far.
        if (!prev_last_slice.empty())
        {
            auto result = std::make_shared<CompressedBuf>(
                decompressBlock(reinterpret_cast<unsigned char *>(prev_last_slice.data()), prev_last_slice.size()));
            const bool was_empty = (curr_result_it == results.end());
            results.push_back(std::move(result));
            if (was_empty)
                curr_result_it = std::prev(results.end());
            prev_last_slice.clear();
            writeToInternal();
        }
        return !working_buffer.empty();
    }

    if (!runner)
    {
        getIOThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        runner = threadPoolCallbackRunnerUnsafe<CompressedBuf>(getIOThreadPool().get(), ThreadName::UNKNOWN);
    }

    in->nextIfAtEnd();
    auto * in_buf = reinterpret_cast<unsigned char *>(in->position());
    size_t in_len = in->buffer().end() - in->position();

    if (!skipped_header_flag)
    {
        /// Skip the 10-byte gzip header written by PigzDeflatingWriteBuffer (no FNAME in this path).
        in_buf += 10;
        in_len -= 10;
        skipped_header_flag = true;
    }

    /// Split the raw-deflate stream on the full-flush markers emitted by the deflater
    /// (00 00 FF FF 00 00 00 FF FF) and schedule decompression of every complete segment.
    static constexpr unsigned char flush_marker[] = {0, 0, 255, 255, 0, 0, 0, 255, 255};
    block_futures.clear();
    bool prev_sep_flag = false;
    size_t prev_sep = 0;
    size_t last_sep = 0;
    for (size_t i = 0; i + sizeof(flush_marker) < in_len; ++i)
    {
        if (memcmp(in_buf + i, flush_marker, sizeof(flush_marker)) != 0)
            continue;

        {
            const size_t curr_sep = i + sizeof(flush_marker);
            last_sep = curr_sep;

            if (!prev_sep_flag)
            {
                /// The first segment of this read completes the tail carried over from the previous read.
                prev_sep_flag = true;
                prev_last_slice.append(reinterpret_cast<char *>(in_buf), curr_sep);
                scheduleDecompressBlock(reinterpret_cast<unsigned char *>(prev_last_slice.data()), prev_last_slice.size());
            }
            else
            {
                scheduleDecompressBlock(in_buf + prev_sep, curr_sep - prev_sep);
            }
            prev_sep = curr_sep;
        }
    }

    if (!prev_sep_flag)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "No deflate block boundary found in the compressed stream");

    in->position() = in->buffer().end();

    /// Wait for all scheduled segments before reusing prev_last_slice or advancing the input buffer.
    appendScheduledResults();

    writeToInternal();

    prev_last_slice.assign(reinterpret_cast<char *>(in_buf + last_sep), in_len - last_sep);

    return true;
}

PigzInflatingReadBuffer::CompressedBuf PigzInflatingReadBuffer::decompressBlock(unsigned char * in_buf, size_t in_len)
{
    /// A segment originates from at most one BLOCK_SIZE chunk of input, so it never expands beyond it.
    const size_t mem_size = PigzDeflatingWriteBuffer::BLOCK_SIZE * 2;
    auto mem = std::make_shared<Memory<>>(mem_size);

    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    infstream.next_in = in_buf;
    infstream.avail_in = static_cast<unsigned>(in_len);
    infstream.next_out = reinterpret_cast<unsigned char *>(mem->data());
    infstream.avail_out = static_cast<unsigned>(mem_size);

    int rc = inflateInit2(&infstream, -15);
    if (rc != Z_OK)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateInit2 failed: {}", zError(rc));

    int inflate_rc = inflate(&infstream, Z_NO_FLUSH);
    if (inflate_rc != Z_OK && inflate_rc != Z_STREAM_END)
    {
        inflateEnd(&infstream);
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflate failed: {}", zError(inflate_rc));
    }

    rc = inflateEnd(&infstream);
    if (rc != Z_OK)
        throw Exception(ErrorCodes::ZLIB_INFLATE_FAILED, "inflateEnd failed: {}", zError(rc));

    return {mem, mem_size - infstream.avail_out, inflate_rc};
}

bool PigzInflatingReadBuffer::writeToInternal()
{
    do
    {
        if (curr_result_it == results.end())
            return true;

        CompressedBuf curr_result = **curr_result_it;
        working_memory = curr_result.mem;
        BufferBase::set(curr_result.mem->data(), curr_result.len, 0);

        ++curr_result_it;
        results.pop_front();
    }
    while (working_buffer.empty());

    return false;
}

void PigzInflatingReadBuffer::scheduleDecompressBlock(unsigned char * in_buf, size_t in_len)
{
    block_futures.push_back(runner(
        [this, in_buf, in_len] { return decompressBlock(in_buf, in_len); },
        Priority{}));
}

void PigzInflatingReadBuffer::appendScheduledResults()
{
    std::exception_ptr exception;
    for (auto & future : block_futures)
    {
        try
        {
            auto result = std::make_shared<CompressedBuf>(future.get());
            const bool was_empty = (curr_result_it == results.end());
            results.push_back(std::move(result));
            if (was_empty)
                curr_result_it = std::prev(results.end());
        }
        catch (...)
        {
            if (!exception)
                exception = std::current_exception();
        }
    }
    block_futures.clear();

    if (exception)
        std::rethrow_exception(exception);
}

}
