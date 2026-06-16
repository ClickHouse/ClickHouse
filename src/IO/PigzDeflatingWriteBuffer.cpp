#include <IO/PigzDeflatingWriteBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/setThreadName.h>

#include <exception>
#include <future>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_DEFLATE_FAILED;
}

void PigzDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    auto * in_buf = reinterpret_cast<unsigned char *>(working_buffer.begin());
    compressAndWrite(in_buf, offset(), false);
}

void PigzDeflatingWriteBuffer::finalFlushBefore()
{
    next();

    auto * in_buf = reinterpret_cast<unsigned char *>(working_buffer.begin());
    compressAndWrite(in_buf, offset(), true);
    writeTrailer();
}

void PigzDeflatingWriteBuffer::writeHeader()
{
    /// gzip member header, RFC 1952 section 2.3.
    out->write(static_cast<char>(0x1f));   /// ID1
    out->write(static_cast<char>(0x8b));   /// ID2
    out->write(static_cast<char>(0x08));   /// CM = deflate

    out->write(static_cast<char>(filename.empty() ? 0x00 : 0x08));   /// FLG: FNAME bit when a file name is present

    /// MTIME = 0 (no timestamp).
    for (size_t i = 0; i < 4; ++i)
        out->write(static_cast<char>(0x00));

    /// XFL: 2 = maximum compression, 4 = fastest.
    out->write(static_cast<char>(compression_level >= 9 ? 0x02 : (compression_level == 1 ? 0x04 : 0x00)));

    out->write(static_cast<char>(0x03));   /// OS = Unix

    if (!filename.empty())
    {
        out->write(filename.data(), filename.size());
        out->write(static_cast<char>(0x00));   /// terminating zero of the FNAME field
    }
}

void PigzDeflatingWriteBuffer::writeTrailer()
{
    /// gzip trailer: CRC-32 and ISIZE (input size modulo 2^32), both little-endian.
    for (size_t i = 0; i < 4; ++i)
        out->write(static_cast<char>((check >> (8 * i)) & 0xff));
    for (size_t i = 0; i < 4; ++i)
        out->write(static_cast<char>((ulen >> (8 * i)) & 0xff));
}

void PigzDeflatingWriteBuffer::deflateEngine(z_stream & strm, WriteBuffer & out_buf, int flush)
{
    do
    {
        out_buf.nextIfAtEnd();
        strm.next_out = reinterpret_cast<unsigned char *>(out_buf.position());
        strm.avail_out = static_cast<unsigned>(out_buf.buffer().end() - out_buf.position());
        deflate(&strm, flush);
        out_buf.position() = out_buf.buffer().end() - strm.avail_out;
    } while (strm.avail_in > 0 || strm.avail_out == 0);
}

PigzDeflatingWriteBuffer::CompressedBuf PigzDeflatingWriteBuffer::compressBlock(unsigned char * in_buf, size_t in_len, bool last_block_flag)
{
    auto mem = std::make_shared<Memory<>>(in_len + 64);
    BufferWithOutsideMemory<WriteBuffer> out_buf(*mem);

    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;

    /// Raw deflate (windowBits = -15): no zlib/gzip wrapper, the wrapper is written by this class.
    int rc = deflateInit2(&strm, compression_level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK)
        throw Exception(ErrorCodes::ZLIB_DEFLATE_FAILED, "deflateInit2 failed: {}; zlib version: {}", zError(rc), ZLIB_VERSION);

    strm.next_in = in_buf;
    strm.avail_in = static_cast<unsigned>(in_len);

    /// Non-final blocks end with a full flush so that they form independent, concatenable raw-deflate
    /// segments that the parallel inflater can split on. The final block closes the stream.
    if (!last_block_flag)
    {
        deflateEngine(strm, out_buf, Z_BLOCK);
        deflateEngine(strm, out_buf, Z_SYNC_FLUSH);
        deflateEngine(strm, out_buf, Z_FULL_FLUSH);
    }
    else
    {
        deflateEngine(strm, out_buf, Z_FINISH);
    }

    deflateEnd(&strm);

    /// out_buf wrote directly into `mem`; finalize() is a no-op flush here but is required by the
    /// WriteBuffer contract before the buffer is destroyed.
    out_buf.finalize();

    return {mem, out_buf.count()};
}

size_t PigzDeflatingWriteBuffer::calcCheck(const unsigned char * buf, size_t len)
{
    return crc32_z(0L, buf, len);
}

void PigzDeflatingWriteBuffer::compressAndWrite(unsigned char * in_buf, size_t in_len, bool final_compression_flag)
{
    ulen += in_len;

    const size_t def_block = BLOCK_SIZE;
    const size_t cnt_blocks = in_len / def_block + static_cast<size_t>(in_len % def_block != 0);

    if (cnt_blocks == 0)
    {
        /// Only reached on the final, empty flush: emit the closing (empty) deflate block.
        if (final_compression_flag)
        {
            CompressedBuf result = compressBlock(in_buf, in_len, true);
            out->write(result.mem->data(), result.len);
        }
        return;
    }

    if (!runner)
    {
        getIOThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        runner = threadPoolCallbackRunnerUnsafe<CompressedBuf>(getIOThreadPool().get(), ThreadName::UNKNOWN);
    }

    /// Schedule deflation of every block on the shared IO thread pool.
    VectorWithMemoryTracking<std::future<CompressedBuf>> futures(cnt_blocks);
    size_t scheduled = 0;
    std::exception_ptr exception;
    try
    {
        for (; scheduled < cnt_blocks; ++scheduled)
        {
            const size_t block_offset = scheduled * def_block;
            const size_t block_len = std::min(def_block, in_len - block_offset);
            unsigned char * block_buf = in_buf + block_offset;
            const bool last_block = final_compression_flag && (block_offset + block_len == in_len);

            futures[scheduled] = runner(
                [this, block_buf, block_len, last_block] { return compressBlock(block_buf, block_len, last_block); },
                Priority{});
        }
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /// Update the running CRC-32 over the whole input on the calling thread,
    /// concurrently with the block deflations scheduled above.
    if (!exception)
        check = crc32_combine(check, calcCheck(in_buf, in_len), static_cast<z_off_t>(in_len));

    /// Collect results in order. Every scheduled task must be awaited before leaving this function,
    /// even on error, because the tasks reference memory owned by this buffer.
    VectorWithMemoryTracking<CompressedBuf> results(scheduled);
    for (size_t i = 0; i < scheduled; ++i)
    {
        try
        {
            results[i] = futures[i].get();
        }
        catch (...)
        {
            if (!exception)
                exception = std::current_exception();
        }
    }

    if (exception)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        std::rethrow_exception(exception);
    }

    for (const auto & result : results)
        out->write(result.mem->data(), result.len);
}

}
