#include <list>

#include <IO/PigzDeflatingWriteBuffer.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ZLIB_DEFLATE_FAILED;
}

PigzDeflatingWriteBuffer::PigzDeflatingWriteBuffer(
    std::unique_ptr<WriteBuffer> out_,
    int compression_level_,
    std::string filename_,
    size_t buf_size,
    char * existing_memory,
    size_t alignment)
    : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment)
    , compression_level(compression_level_)
    , filename(filename_)
{
}

const size_t MAXP2 = UINT_MAX - (UINT_MAX >> 1);
const size_t BLOCK = 131072;

void PigzDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    writeHeader();

    auto *in_buf = reinterpret_cast<unsigned char *>(working_buffer.begin());
    size_t in_len = offset();
    uint64_t check = crc32_z(0L, Z_NULL, 0);

    size_t cnt_blocks = in_len / BLOCK + bool(in_len % BLOCK);
    std::vector<CompressedBuf> results(cnt_blocks);
    std::vector<size_t> checks(cnt_blocks);
    std::vector<size_t> blocks(cnt_blocks);

    ThreadPool pool(15);
    try
    {
        for (size_t i = 0; i < cnt_blocks; ++i)
        {
            size_t in_remaining_len = in_len - i * BLOCK;
            size_t block = std::min(BLOCK, in_remaining_len);

            blocks[i] = block;

            unsigned char *in_slice_buf = in_buf + (in_len - in_remaining_len);

            pool.scheduleOrThrowOnError(
                [&, i = i, in_remaining_len = in_remaining_len, block = block, in_slice_buf = in_slice_buf]
                {
                    results[i] = compressSlice(in_slice_buf, block, block == in_remaining_len);
                });

            pool.scheduleOrThrowOnError(
                [&, i = i, block = block, in_slice_buf = in_slice_buf]
                {
                    checks[i] = calcCheck(in_slice_buf, block);
                });

        }
        pool.wait();

        for (size_t i = 0; i < cnt_blocks; ++i)
            check = crc32_combine(check, checks[i], blocks[i]);

        for (const auto & result : results)
            out->write(result.mem->data(), result.len);

        writeTrailer(in_len, check);
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

PigzDeflatingWriteBuffer::~PigzDeflatingWriteBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void PigzDeflatingWriteBuffer::finalizeBefore()
{
    next();
}

void PigzDeflatingWriteBuffer::finalizeAfter()
{
}

void PigzDeflatingWriteBuffer::writeHeader()
{
    out->write(31);
    out->write(139);
    out->write(8);

    char with_name_byte = 0;
    if (!filename.empty())
        with_name_byte = 8;
    out->write(with_name_byte);

    // TODO: time
    out->write(0);
    out->write(0);
    out->write(0);
    out->write(0);

    out->write(compression_level >= 9 ? 2 : compression_level == 1 ? 4 : 0);

    out->write(3);

    if (!filename.empty())
    {
        out->write(&filename.front(), filename.size());
        out->write(0);
    }
}

void PigzDeflatingWriteBuffer::writeTrailer(uintmax_t ulen, uint64_t check)
{
    for (size_t i = 0; i < 4; ++i)
    {
        out->write((check >> sizeof(check) * i) & 0xff);
    }
    for (size_t i = 0; i < 4; ++i)
    {
        out->write((ulen >> sizeof(ulen) * i) & 0xff);
    }
}

void PigzDeflatingWriteBuffer::deflateEngine(z_stream & strm, WriteBuffer & out_buf, int flush)
{
    do
    {
        out_buf.nextIfAtEnd();
        strm.next_out = reinterpret_cast<unsigned char *>(out_buf.position());
        strm.avail_out = out_buf.buffer().end() - out_buf.position();
        deflate(&strm, flush);
        out_buf.position() = out_buf.buffer().end() - strm.avail_out;
    } while (strm.avail_in > 0 || strm.avail_out == 0);
}

PigzDeflatingWriteBuffer::CompressedBuf PigzDeflatingWriteBuffer::compressSlice(unsigned char * in_buf, size_t in_len, bool last_block_flag)
{
    // TODO: понять сколько нужно памяти и вынести в константу (должны сократится аллокации)
    auto mem = std::make_shared<Memory<>>(10);
    BufferWithOutsideMemory<WriteBuffer> out_buf(*mem);

    auto strategy = Z_DEFAULT_STRATEGY;

    z_stream strm;
    strm.zfree = Z_NULL;
    strm.zalloc = Z_NULL;
    strm.opaque = Z_NULL;

    int rc = deflateInit2(&strm, compression_level, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
    if (rc != Z_OK)
        throw Exception(std::string("deflate failed: ") + zError(rc), ErrorCodes::ZLIB_DEFLATE_FAILED);

    deflateReset(&strm);
    deflateParams(&strm, compression_level, strategy);

    strm.next_in = in_buf;
    strm.avail_in = in_len;

    if (!last_block_flag)
    {
        deflateEngine(strm, out_buf, Z_BLOCK);
        // TODO: добавить обработку bits
        int bits;
        deflatePending(&strm, Z_NULL, &bits);
        deflateEngine(strm, out_buf, Z_SYNC_FLUSH);
        deflateEngine(strm, out_buf, Z_FULL_FLUSH);
    }
    else
    {
        deflateEngine(strm, out_buf, Z_FINISH);
    }

    deflateEnd(&strm);

    return {mem, out_buf.count()};
}

size_t PigzDeflatingWriteBuffer::calcCheck(unsigned char * buf_, size_t len_)
{
    unsigned char * check_buff = buf_;
    size_t len = len_;

    size_t curr_check = crc32_z(0L, Z_NULL, 0);
    while (len > MAXP2)
    {
        curr_check = crc32_z(curr_check, check_buff, MAXP2);
        len -= MAXP2;
        check_buff += MAXP2;
    }
    return crc32_z(curr_check, check_buff, len);
}

}
