#include <Formats/PNGWriter.h>

#include <array>
#include <limits>

#include <zlib.h>

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ZlibDeflatingWriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Deflate level for the image data. 6 is zlib's default: a good size/speed balance.
    constexpr int COMPRESSION_LEVEL = 6;

    /// Emit the compressed pixel stream in `IDAT` chunks of at most this many bytes. PNG allows the Deflate
    /// datastream to be split across any number of `IDAT` chunks, so bounding the chunk size keeps peak memory
    /// constant regardless of the image size. The value only trades a little per-chunk overhead (12 bytes each)
    /// against memory; it does not affect the decoded image.
    constexpr size_t IDAT_CHUNK_SIZE = 1 << 16; /// 64 KiB

    /// Write one PNG chunk: 4-byte big-endian length, 4-byte type, the data,
    /// and the 4-byte big-endian CRC-32 of the type followed by the data.
    void writeChunk(WriteBuffer & out, const char (&type)[5], const char * data, size_t size)
    {
        if (size > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PNG chunk is too large ({} bytes)", size);

        writeBinaryBigEndian(static_cast<UInt32>(size), out);
        out.write(type, 4);
        if (size)
            out.write(data, size);

        uLong crc = crc32_z(0, reinterpret_cast<const Bytef *>(type), 4);
        if (size)
            crc = crc32_z(crc, reinterpret_cast<const Bytef *>(data), size);
        writeBinaryBigEndian(static_cast<UInt32>(crc), out);
    }

    /// A sink for the Deflate-compressed pixel stream that flushes it to `out` as a sequence of bounded
    /// `IDAT` chunks. Every time this fixed-size buffer fills up (or is finalized) its contents are emitted
    /// as one more `IDAT` chunk, so we never materialize the whole compressed image in memory and the chunks
    /// stream out to `out` (HTTP, file, ...) as compression proceeds. Peak memory stays bounded by the buffer
    /// size, independent of the image dimensions.
    class IDATChunkWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
    {
    public:
        IDATChunkWriteBuffer(WriteBuffer & out_, size_t buf_size)
            : BufferWithOwnMemory<WriteBuffer>(buf_size), out(out_)
        {
        }

    private:
        void nextImpl() override
        {
            if (offset() == 0)
                return;
            writeChunk(out, "IDAT", working_buffer.begin(), offset());
        }

        WriteBuffer & out;
    };

    /// The five PNG scanline filter types (PNG spec, section 6). Each replaces every byte of the row by the
    /// difference between it and a predictor computed from already-decoded neighbours, which makes the row
    /// more compressible for Deflate without losing any information (the decoder undoes the filter).
    enum class Filter : UInt8
    {
        None = 0,     /// no prediction: the raw byte
        Sub = 1,      /// predict from the byte to the left
        Up = 2,       /// predict from the byte above
        Average = 3,  /// predict from the average of the left and above bytes
        Paeth = 4,    /// predict with the Paeth predictor over the left, above and above-left bytes
    };

    constexpr std::array<Filter, 5> ALL_FILTERS
        = {Filter::None, Filter::Sub, Filter::Up, Filter::Average, Filter::Paeth};

    /// The Paeth predictor (PNG spec, section 6.6): of the left (`a`), above (`b`) and above-left (`c`)
    /// bytes, return the one closest to `a + b - c` (the linear estimate), preferring `a`, then `b`, then `c`.
    UInt8 paethPredictor(UInt8 a, UInt8 b, UInt8 c)
    {
        const int p = static_cast<int>(a) + static_cast<int>(b) - static_cast<int>(c);
        const int pa = p >= a ? p - a : a - p;
        const int pb = p >= b ? p - b : b - p;
        const int pc = p >= c ? p - c : c - p;
        if (pa <= pb && pa <= pc)
            return a;
        if (pb <= pc)
            return b;
        return c;
    }

    /// Apply one filter to `raw` (the current scanline of `row_bytes` bytes), given `prior` (the previous
    /// scanline, or all zeros for the first row) and `bpp` (bytes per pixel), writing the filtered bytes to
    /// `dst`. The filtered byte is `raw[i] - predictor` taken modulo 256, exactly as the decoder inverts it.
    void applyFilter(Filter filter, const UInt8 * raw, const UInt8 * prior, size_t row_bytes, size_t bpp, UInt8 * dst)
    {
        for (size_t i = 0; i < row_bytes; ++i)
        {
            const UInt8 a = i >= bpp ? raw[i - bpp] : 0;    /// byte to the left
            const UInt8 b = prior[i];                       /// byte above
            const UInt8 c = i >= bpp ? prior[i - bpp] : 0;  /// byte above-left

            UInt8 predictor = 0;
            switch (filter)
            {
                case Filter::None: predictor = 0; break;
                case Filter::Sub: predictor = a; break;
                case Filter::Up: predictor = b; break;
                case Filter::Average: predictor = static_cast<UInt8>((static_cast<int>(a) + static_cast<int>(b)) / 2); break;
                case Filter::Paeth: predictor = paethPredictor(a, b, c); break;
            }
            dst[i] = static_cast<UInt8>(raw[i] - predictor);
        }
    }

    /// The "minimum sum of absolute differences" heuristic recommended by the PNG spec for choosing a filter:
    /// interpret each filtered byte as a signed value and sum the absolute values. A smaller sum means bytes
    /// closer to zero, which Deflate compresses better, so the row filter with the smallest sum tends to give
    /// the smallest output. It is the same default heuristic the removed `libpng` path used.
    UInt64 filteredRowScore(const UInt8 * data, size_t row_bytes)
    {
        UInt64 sum = 0;
        for (size_t i = 0; i < row_bytes; ++i)
        {
            const UInt8 v = data[i];
            sum += v < 128 ? v : 256u - v;
        }
        return sum;
    }
}

PNGWriter::PNGWriter(WriteBuffer & out_, size_t width_, size_t height_, size_t channels_)
    : out(out_)
    , width(width_)
    , height(height_)
    , channels(channels_)
{
    if (channels != 1 && channels != 3 && channels != 4)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "PNG writer supports only 1, 3, or 4 channels per pixel, got {}", channels);

    /// PNG stores width and height as 4-byte unsigned integers and disallows zero.
    static constexpr size_t MAX_DIMENSION = 0x7fffffff;
    if (width == 0 || height == 0 || width > MAX_DIMENSION || height > MAX_DIMENSION)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "PNG image dimensions must be between 1 and {} (got {}x{})", MAX_DIMENSION, width, height);
}

void PNGWriter::writeImage(const unsigned char * pixels)
{
    if (written)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PNG writer can encode only one image");
    written = true;

    /// The 8-byte PNG file signature.
    static constexpr std::array<char, 8> signature
        = {static_cast<char>(0x89), 'P', 'N', 'G', '\r', '\n', static_cast<char>(0x1A), '\n'};
    out.write(signature.data(), signature.size());

    /// IHDR: image header (13 bytes).
    UInt8 color_type = 0;
    switch (channels)
    {
        case 1: color_type = 0; break; /// grayscale
        case 3: color_type = 2; break; /// RGB
        case 4: color_type = 6; break; /// RGBA
        default: break; /// unreachable: the number of channels is validated in the constructor
    }

    auto put_be32 = [](char * p, UInt32 value)
    {
        p[0] = static_cast<char>(value >> 24);
        p[1] = static_cast<char>(value >> 16);
        p[2] = static_cast<char>(value >> 8);
        p[3] = static_cast<char>(value);
    };

    char ihdr[13];
    put_be32(ihdr, static_cast<UInt32>(width));
    put_be32(ihdr + 4, static_cast<UInt32>(height));
    ihdr[8] = 8;                              /// bit depth
    ihdr[9] = static_cast<char>(color_type);  /// color type
    ihdr[10] = 0;                             /// compression method: Deflate
    ihdr[11] = 0;                             /// filter method: the standard set of five per-scanline filters
    ihdr[12] = 0;                             /// interlace method: none
    writeChunk(out, "IHDR", ihdr, sizeof(ihdr));

    /// IDAT: the pixel data, Deflate-compressed. Each scanline is prefixed with a single filter-type byte and
    /// then filtered. For every scanline we try all five PNG filters and keep the one whose filtered bytes have
    /// the smallest sum of absolute values (the heuristic recommended by the PNG spec, and the default the
    /// removed `libpng` path used): this keeps the output small on filter-friendly images such as gradients or
    /// photos, instead of always emitting the raw ("None") bytes. The compressed stream is emitted as bounded
    /// `IDAT` chunks straight to `out`, so peak memory does not grow with the image size and the output streams
    /// as it is produced.
    {
        IDATChunkWriteBuffer idat(out, IDAT_CHUNK_SIZE);
        ZlibDeflatingWriteBuffer deflate(&idat, CompressionMethod::Zlib, COMPRESSION_LEVEL);

        const size_t row_bytes = width * channels;
        const size_t bpp = channels; /// bytes per pixel (bit depth is fixed at 8)

        /// Scratch buffers, all one scanline wide, so peak memory stays bounded regardless of the height:
        /// the best filtered row so far, the candidate currently being tried, and an all-zero "prior" row for
        /// the first scanline (later scanlines use the previous raw row, which is already in `pixels`).
        /// They use `VectorWithMemoryTracking` (the throwing allocator) so that these user-controlled
        /// (`width * channels`) allocations honor `max_memory_usage` like the rest of the encoder.
        VectorWithMemoryTracking<UInt8> best(row_bytes);
        VectorWithMemoryTracking<UInt8> candidate(row_bytes);
        const VectorWithMemoryTracking<UInt8> zero_row(row_bytes, 0);

        const auto * image = reinterpret_cast<const UInt8 *>(pixels);
        for (size_t y = 0; y < height; ++y)
        {
            const UInt8 * raw = image + y * row_bytes;
            const UInt8 * prior = y == 0 ? zero_row.data() : image + (y - 1) * row_bytes;

            Filter best_filter = Filter::None;
            UInt64 best_score = std::numeric_limits<UInt64>::max();
            for (Filter filter : ALL_FILTERS)
            {
                applyFilter(filter, raw, prior, row_bytes, bpp, candidate.data());
                const UInt64 score = filteredRowScore(candidate.data(), row_bytes);
                if (score < best_score)
                {
                    best_score = score;
                    best_filter = filter;
                    best.swap(candidate);
                }
            }

            const char filter_byte = static_cast<char>(best_filter);
            deflate.write(&filter_byte, 1);
            deflate.write(reinterpret_cast<const char *>(best.data()), row_bytes);
        }
        deflate.finalize(); /// Flushes the Deflate stream, emitting the final `IDAT` chunk, and finalizes `idat`.
    }

    /// IEND: end of the image.
    writeChunk(out, "IEND", nullptr, 0);
}

void PNGWriter::finalize()
{
    out.next();
}

}
