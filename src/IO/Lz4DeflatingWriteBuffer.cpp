#include <IO/Lz4DeflatingWriteBuffer.h>
#include <Common/Exception.h>


namespace
{
    using namespace DB;

    /// SinkToOut provides the safe way to do direct write into buffer's memory
    /// When out->capacity() is not less than guaranteed_capacity, SinkToOut is pointing directly to out_'s memory.
    /// Otherwise the writes are directed to the temporary memory. That data is copied to out_ at finalize call.
    class SinkToOut
    {
    public:
        SinkToOut(WriteBuffer * out_, Memory<> & mem_, size_t guaranteed_capacity)
            : sink(out_)
            , tmp_out(mem_)
            , cur_out(sink)
        {
            chassert(sink);

            if (sink->available() < guaranteed_capacity)
            {
                mem_.resize(guaranteed_capacity);
                cur_out = &tmp_out;
            }
        }

        size_t getCapacity()
        {
           return  cur_out->available();
        }

        BufferBase::Position getPosition()
        {
            return cur_out->position();
        }

        void advancePosition(size_t size)
        {
            chassert(size <= cur_out->available());
            cur_out->position() += size;
        }

        void finalize()
        {
            tmp_out.finalize();
            sink->write(tmp_out.buffer().begin(), tmp_out.count());
        }

    private:
        WriteBuffer * sink;
        BufferWithOutsideMemory<WriteBuffer> tmp_out;
        WriteBuffer * cur_out;
    };
}


namespace DB
{
namespace ErrorCodes
{
    extern const int LZ4_ENCODER_FAILED;
}


void Lz4DeflatingWriteBuffer::initialize(int compression_level)
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
        1, /* autoflush */
        0, /* favor decompression speed */
        {0, 0, 0}, /* reserved, must be set to 0 */
    };

    size_t ret = LZ4F_createCompressionContext(&ctx, LZ4F_VERSION);

    if (LZ4F_isError(ret))
        throw Exception(
            ErrorCodes::LZ4_ENCODER_FAILED,
            "creation of LZ4 compression context failed. LZ4F version: {}, error: {}",
            LZ4F_VERSION, LZ4F_getErrorName(ret));
}

Lz4DeflatingWriteBuffer::~Lz4DeflatingWriteBuffer()
{
    if (ctx)
        LZ4F_freeCompressionContext(ctx);
}

void Lz4DeflatingWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    if (first_time)
    {
        auto sink = SinkToOut(out, tmp_memory, LZ4F_HEADER_SIZE_MAX);
        chassert(sink.getCapacity() >= LZ4F_HEADER_SIZE_MAX);

        /// write frame header and check for errors
        size_t header_size = LZ4F_compressBegin(
                ctx, sink.getPosition(), sink.getCapacity(), &kPrefs);

        if (LZ4F_isError(header_size))
            throw Exception(
                ErrorCodes::LZ4_ENCODER_FAILED,
                "LZ4 failed to start stream encoding. LZ4F version: {}, error: {}",
                LZ4F_VERSION, LZ4F_getErrorName(header_size));

        sink.advancePosition(header_size);
        sink.finalize();
        first_time = false;
    }

    auto * in_data = working_buffer.begin();
    auto in_capacity = offset();

    while (in_capacity > 0)
    {
        /// Ensure that there is enough space for compressed block of minimal size
        size_t min_compressed_block_size = LZ4F_compressBound(1, &kPrefs);

        auto sink = SinkToOut(out, tmp_memory, min_compressed_block_size);
        chassert(sink.getCapacity() >= min_compressed_block_size);

        /// LZ4F_compressUpdate compresses whole input buffer at once so we need to shink it manually
        size_t cur_buffer_size = in_capacity;
        if (sink.getCapacity() >= min_compressed_block_size) /// We cannot shrink the input buffer if it's already too small.
        {
            while (sink.getCapacity() < LZ4F_compressBound(cur_buffer_size, &kPrefs))
                cur_buffer_size /= 2;
        }

        size_t compressed_size = LZ4F_compressUpdate(
                ctx, sink.getPosition(), sink.getCapacity(), in_data, cur_buffer_size, nullptr);

        if (LZ4F_isError(compressed_size))
            throw Exception(
                ErrorCodes::LZ4_ENCODER_FAILED,
                "LZ4 failed to encode stream. LZ4F version: {}, error {}, out_capacity {}",
                LZ4F_VERSION, LZ4F_getErrorName(compressed_size), sink.getCapacity());

        in_capacity -= cur_buffer_size;
        in_data += cur_buffer_size;

        sink.advancePosition(compressed_size);
        sink.finalize();
    }
}

void Lz4DeflatingWriteBuffer::finalizeBefore()
{
    next();

    /// Don't write out if no data was ever compressed
    if (!compress_empty && first_time)
        return;

    auto suffix_size = LZ4F_compressBound(0, &kPrefs);
    auto sink = SinkToOut(out, tmp_memory, suffix_size);
    chassert(sink.getCapacity() >= suffix_size);

    /// compression end
    size_t end_size = LZ4F_compressEnd(ctx, sink.getPosition(), sink.getCapacity(), nullptr);

    if (LZ4F_isError(end_size))
        throw Exception(
            ErrorCodes::LZ4_ENCODER_FAILED,
            "LZ4 failed to end stream encoding. LZ4F version: {}, error {}, out_capacity {}",
            LZ4F_VERSION, LZ4F_getErrorName(end_size), sink.getCapacity());

    sink.advancePosition(end_size);
    sink.finalize();
}

void Lz4DeflatingWriteBuffer::finalizeAfter()
{
    LZ4F_freeCompressionContext(ctx);
    ctx = nullptr;
}

}
