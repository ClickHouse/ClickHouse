#include <IO/PixzDeflatingWriteBuffer.h>
#include <IO/SharedThreadPools.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/setThreadName.h>

#include <algorithm>
#include <cstring>
#include <exception>
#include <future>


namespace DB
{

namespace ErrorCodes
{
    extern const int LZMA_STREAM_ENCODER_FAILED;
}

void PixzDeflatingWriteBuffer::init(int compression_level)
{
    if (lzma_lzma_preset(&lzma_opts, compression_level))
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma preset failed: lzma version: {}", LZMA_VERSION_STRING);

    gFilters[0] = lzma_filter{.id = LZMA_FILTER_LZMA2, .options = &lzma_opts};
    gFilters[1] = lzma_filter{.id = LZMA_VLI_UNKNOWN, .options = nullptr};
    gBlockInSize = static_cast<size_t>(lzma_opts.dict_size * gBlockFraction);
    gBlockOutSize = lzma_block_buffer_bound(gBlockInSize);

    gIndex = lzma_index_init(nullptr);
    if (!gIndex)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma index init failed: lzma version: {}", LZMA_VERSION_STRING);

    writeHeader();
}

PixzDeflatingWriteBuffer::~PixzDeflatingWriteBuffer()
{
    /// It is OK to call lzma_end() again here (writeTrailer() already did under normal finalization).
    lzma_end(&gStream);
    if (gIndex)
        lzma_index_end(gIndex, nullptr);
}

void PixzDeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    auto * in_buf = reinterpret_cast<uint8_t *>(working_buffer.begin());
    const size_t in_len = offset();

    const size_t def_block_len = BLOCK_SIZE;
    const size_t cnt_blocks = in_len / def_block_len + static_cast<size_t>(in_len % def_block_len != 0);
    if (cnt_blocks == 0)
        return;

    if (!runner)
    {
        getIOThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        runner = threadPoolCallbackRunnerUnsafe<CompressedBuf>(getIOThreadPool().get(), ThreadName::UNKNOWN);
    }

    /// Encode every block on the shared IO thread pool.
    VectorWithMemoryTracking<std::future<CompressedBuf>> futures(cnt_blocks);
    size_t scheduled = 0;
    std::exception_ptr exception;
    try
    {
        for (; scheduled < cnt_blocks; ++scheduled)
        {
            const size_t block_offset = scheduled * def_block_len;
            const size_t block_len = std::min(def_block_len, in_len - block_offset);
            uint8_t * block_buf = in_buf + block_offset;

            futures[scheduled] = runner(
                [this, block_buf, block_len] { return compressBlock(block_buf, block_len); },
                Priority{});
        }
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /// Every scheduled task must be awaited before leaving this function, even on error,
    /// because the tasks reference memory owned by this buffer.
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

    try
    {
        if (exception)
            std::rethrow_exception(exception);

        /// The index is shared and not thread-safe, so it is filled sequentially here.
        for (const auto & result : results)
        {
            out->write(result.mem->data(), result.len);
            lzma_ret ret = lzma_index_append(gIndex, nullptr, lzma_block_unpadded_size(&result.block), result.block.uncompressed_size);
            if (ret != LZMA_OK)
                throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma index append failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);
        }
    }
    catch (...)
    {
        /// Do not try to write next time after exception.
        out->position() = out->buffer().begin();
        throw;
    }
}

void PixzDeflatingWriteBuffer::finalFlushBefore()
{
    next();

    auto * in_buf = reinterpret_cast<uint8_t *>(working_buffer.begin());
    const size_t in_len = offset();

    /// Data is flushed by next() above; only emit a trailing block if something is left.
    if (in_len > 0)
    {
        CompressedBuf result = compressBlock(in_buf, in_len);
        out->write(result.mem->data(), result.len);
        lzma_ret ret = lzma_index_append(gIndex, nullptr, lzma_block_unpadded_size(&result.block), result.block.uncompressed_size);
        if (ret != LZMA_OK)
            throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma index append failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);
    }

    writeTrailer(lzma_index_size(gIndex));
}

void PixzDeflatingWriteBuffer::writeHeader()
{
    lzma_stream_flags flags;
    flags.version = 0;
    flags.check = LZMA_CHECK_CRC32;
    flags.backward_size = LZMA_VLI_UNKNOWN;
    uint8_t buf[LZMA_STREAM_HEADER_SIZE];

    lzma_ret ret = lzma_stream_header_encode(&flags, buf);
    if (ret != LZMA_OK)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma stream header encode failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);

    out->write(reinterpret_cast<char *>(buf), LZMA_STREAM_HEADER_SIZE);
}

void PixzDeflatingWriteBuffer::writeTrailer(lzma_vli backward_size)
{
    lzma_ret ret = lzma_index_encoder(&gStream, gIndex);
    if (ret != LZMA_OK)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma index encoder failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);

    uint8_t obuf[CHUNKSIZE];
    while (ret != LZMA_STREAM_END)
    {
        gStream.next_out = obuf;
        gStream.avail_out = CHUNKSIZE;
        ret = lzma_code(&gStream, LZMA_RUN);
        if (ret != LZMA_OK && ret != LZMA_STREAM_END)
            throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma code failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);
        if (gStream.avail_out != CHUNKSIZE)
            out->write(reinterpret_cast<char *>(obuf), CHUNKSIZE - gStream.avail_out);
    }
    lzma_end(&gStream);

    lzma_stream_flags flags;
    flags.version = 0;
    flags.check = LZMA_CHECK_CRC32;
    flags.backward_size = backward_size;
    uint8_t buf[LZMA_STREAM_HEADER_SIZE];

    ret = lzma_stream_footer_encode(&flags, buf);
    if (ret != LZMA_OK)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma stream footer encode failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);

    out->write(reinterpret_cast<char *>(buf), LZMA_STREAM_HEADER_SIZE);
}

PixzDeflatingWriteBuffer::CompressedBuf PixzDeflatingWriteBuffer::compressBlock(uint8_t * block_buf, size_t block_len)
{
    lzma_stream stream = LZMA_STREAM_INIT;
    lzma_block block = createBlock(block_len);

    size_t header_size = block.header_size;
    size_t compressed_len = calcCompressedSize(block_len) + lzma_check_size(block.check);

    auto mem = std::make_shared<Memory<>>(header_size + compressed_len);
    auto * out_data = reinterpret_cast<uint8_t *>(mem->data());

    lzma_ret ret = lzma_block_encoder(&stream, &block);
    if (ret != LZMA_OK)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma block encoder failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);

    stream.next_in = block_buf;
    stream.avail_in = block_len;
    stream.next_out = out_data + header_size;
    stream.avail_out = compressed_len;

    block.uncompressed_size = LZMA_VLI_UNKNOWN;
    lzma_ret encode_ret = LZMA_OK;
    while (encode_ret == LZMA_OK)
        encode_ret = lzma_code(&stream, LZMA_FINISH);

    size_t out_size = 0;
    if (encode_ret == LZMA_BUF_ERROR)
    {
        encodeUncompressible(&block, block_buf, block_len, out_data);
        out_size = header_size + compressed_len;
    }
    else if (encode_ret == LZMA_STREAM_END)
    {
        out_size = stream.next_out - out_data;
    }
    else
    {
        lzma_end(&stream);
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma code failed: error code: {}; lzma version: {}", static_cast<int>(encode_ret), LZMA_VERSION_STRING);
    }

    ret = lzma_block_header_encode(&block, out_data);
    if (ret != LZMA_OK)
    {
        lzma_end(&stream);
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma block header encode failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);
    }

    lzma_end(&stream);

    return {mem, out_size, block};
}

lzma_block PixzDeflatingWriteBuffer::createBlock(size_t block_len)
{
    lzma_block block;

    block.version = 0;
    block.check = LZMA_CHECK_CRC32;
    block.filters = gFilters;
    block.uncompressed_size = block_len ? block_len : LZMA_VLI_UNKNOWN;
    block.compressed_size = block_len ? gBlockOutSize : LZMA_VLI_UNKNOWN;

    lzma_ret ret = lzma_block_header_size(&block);
    if (ret != LZMA_OK)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "lzma block header size failed: error code: {}; lzma version: {}", static_cast<int>(ret), LZMA_VERSION_STRING);

    return block;
}

size_t PixzDeflatingWriteBuffer::calcCompressedSize(size_t block_len)
{
    size_t chunks = block_len / LZMA_CHUNK_MAX;
    if (block_len % LZMA_CHUNK_MAX)
        ++chunks;
    size_t data_size = block_len + chunks * 3 + 1;
    if (data_size % 4)
        data_size += 4 - data_size % 4;
    return data_size;
}

void PixzDeflatingWriteBuffer::encodeUncompressible(lzma_block * block, uint8_t * in_data, size_t in_size, uint8_t * out_data)
{
    const uint8_t control_uncomp = 1;
    const uint8_t control_end = 0;

    uint8_t * output_start = out_data + block->header_size;
    uint8_t * output = output_start;
    uint8_t * input = in_data;
    size_t remain = in_size;

    while (remain)
    {
        size_t size = std::min(remain, LZMA_CHUNK_MAX);

        *output++ = control_uncomp;

        uint16_t size_write = static_cast<uint16_t>(size - 1);
        *output++ = static_cast<uint8_t>(size_write >> 8);
        *output++ = static_cast<uint8_t>(size_write & 0xFF);

        memcpy(output, input, size);

        remain -= size;
        output += size;
        input += size;
    }
    *output++ = control_end;

    block->compressed_size = output - output_start;
    block->uncompressed_size = in_size;

    while ((output - output_start) % 4)
        *output++ = 0;

    if (block->check != LZMA_CHECK_CRC32)
        throw Exception(ErrorCodes::LZMA_STREAM_ENCODER_FAILED, "pixz only supports CRC-32 checksums: lzma version: {}", LZMA_VERSION_STRING);
    uint32_t check = lzma_crc32(in_data, in_size, 0);
    *output++ = static_cast<uint8_t>(check & 0xFF);
    *output++ = static_cast<uint8_t>((check >> 8) & 0xFF);
    *output++ = static_cast<uint8_t>((check >> 16) & 0xFF);
    *output++ = static_cast<uint8_t>(check >> 24);
}

}
