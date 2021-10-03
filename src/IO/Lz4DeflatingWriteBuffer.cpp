#include <IO/Lz4DeflatingWriteBuffer.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LZ4_ENCODER_FAILED;
}

Lz4DeflatingWriteBuffer::Lz4DeflatingWriteBuffer(
    std::unique_ptr<WriteBuffer> out_, int compression_level, size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
    , out(std::move(out_))
    , in_data(nullptr)
    , out_data(nullptr)
    , in_capacity(0)
    , out_capacity(0)
{
    kPrefs = {
        {LZ4F_max256KB,
         LZ4F_blockLinked,
         LZ4F_noContentChecksum,
         LZ4F_frame,
         0 /* unknown content size */,
         0 /* no dictID */,
         LZ4F_noBlockChecksum},
        compression_level, /* compression level; 0 == default */
        0, /* autoflush */
        0, /* favor decompression speed */
        {0, 0, 0}, /* reserved, must be set to 0 */
    };

    size_t ret = LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);

    if (LZ4F_isError(ret))
        throw Exception(
            ErrorCodes::LZ4_ENCODER_FAILED,
            "creation of LZ4 compression context failed. LZ4F version: {}",
            LZ4F_VERSION,
            ErrorCodes::LZ4_ENCODER_FAILED);
}

Lz4DeflatingWriteBuffer::~Lz4DeflatingWriteBuffer()
{
    MemoryTracker::LockExceptionInThread lock(VariableContext::Global);
    finish();
    LZ4F_freeCompressionContext(ctx);
}

void Lz4DeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    in_data = reinterpret_cast<void *>(working_buffer.begin());
    in_capacity = offset();

    try
    {
        if (first_time)
        {
            out->nextIfAtEnd();

            out_data = reinterpret_cast<void *>(out->position());
            out_capacity = out->buffer().end() - out->position();

            /// write frame header and check for errors
            size_t header_size = LZ4F_compressBegin(ctx, out_data, out_capacity, &kPrefs);

            if (LZ4F_isError(header_size))
                throw Exception(
                    ErrorCodes::LZ4_ENCODER_FAILED,
                    "LZ4 failed to start stream encoding. LZ4F version: {}",
                    LZ4F_VERSION);

            out_capacity -= header_size;
            out->position() = out->buffer().end() - out_capacity;
            first_time = false;
        }

        do
        {
            /// Ensure that there is enough space for compressed block of minimal size
            if (out_capacity < LZ4F_compressBound(0, &kPrefs))
            {
                out->next();
                out_capacity = out->buffer().end() - out->position();
            }

            out_data = reinterpret_cast<void *>(out->position());

            /// LZ4F_compressUpdate compresses whole input buffer at once so we need to shink it manually
            size_t cur_buffer_size = in_capacity;
            while (out_capacity < LZ4F_compressBound(cur_buffer_size, &kPrefs))
                cur_buffer_size /= 2;

            size_t compressed_size = LZ4F_compressUpdate(ctx, out_data, out_capacity, in_data, cur_buffer_size, nullptr);

            if (LZ4F_isError(compressed_size))
                throw Exception(
                    ErrorCodes::LZ4_ENCODER_FAILED,
                    "LZ4 failed to encode stream. LZ4F version: {}",
                    LZ4F_VERSION);

            out_capacity -= compressed_size;
            in_capacity -= cur_buffer_size;

            in_data = reinterpret_cast<void *>(working_buffer.end() - in_capacity);
            out->position() = out->buffer().end() - out_capacity;
        }
        while (in_capacity > 0);
    }
    catch (...)
    {
        out->position() = out->buffer().begin();
        throw;
    }
}

void Lz4DeflatingWriteBuffer::finish()
{
    if (finished)
        return;

    try
    {
        finishImpl();
        out->finalize();
        finished = true;
    }
    catch (...)
    {
        /// Do not try to flush next time after exception.
        out->position() = out->buffer().begin();
        finished = true;
        throw;
    }
}

void Lz4DeflatingWriteBuffer::finishImpl()
{
    next();

    if (out_capacity < LZ4F_compressBound(0, &kPrefs))
    {
        out->next();
        out_capacity = out->buffer().end() - out->position();
    }

    out_data = reinterpret_cast<void *>(out->position());

    /// compression end
    size_t end_size = LZ4F_compressEnd(ctx, out_data, out_capacity, nullptr);

    if (LZ4F_isError(end_size))
        throw Exception(
            ErrorCodes::LZ4_ENCODER_FAILED,
            "LZ4 failed to end stream encoding. LZ4F version: {}",
            LZ4F_VERSION);

    out_capacity -= end_size;
    out->position() = out->buffer().end() - out_capacity;
}

}
