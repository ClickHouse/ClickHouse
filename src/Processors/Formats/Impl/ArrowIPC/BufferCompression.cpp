#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>

#if USE_ARROW

#include <Common/Exception.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>
#include <algorithm>
#include <cstring>

/// For `ZSTD_findDecompressedSize`, which `zstd.h` exposes only to static linkers.
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <lz4frame.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}
}

namespace DB::ArrowIPC
{

namespace
{
/// Reads a little-endian unsigned of `Bytes` from `p`, which must hold that many bytes.
template <size_t Bytes>
UInt64 readLittleEndian(const char * p)
{
    UInt64 v = 0;
    for (size_t i = 0; i < Bytes; ++i)
        v |= static_cast<UInt64>(static_cast<unsigned char>(p[i])) << (8 * i);
    return v;
}

/// The most the blocks of one LZ4 frame can expand to, without decompressing any of them: each
/// block's output is capped both by the frame's maximum block size and by what its own stored bytes
/// can encode, and those per-block caps summed bound the frame. `header_size` is where its blocks begin.
UInt64 lz4BlockOutputBound(const LZ4F_frameInfo_t & info, const char * src, size_t size, size_t header_size)
{
    /// `LZ4F_getBlockSize` maps these, but is declared only in the static-linking section of
    /// `lz4frame.h`, which does not compile here.
    UInt64 max_block_size = 0;
    switch (info.blockSizeID)
    {
        case LZ4F_max64KB: max_block_size = 64 * 1024; break;
        case LZ4F_max256KB: max_block_size = 256 * 1024; break;
        case LZ4F_max1MB: max_block_size = 1024 * 1024; break;
        case LZ4F_max4MB: max_block_size = 4 * 1024 * 1024; break;
        /// The parsed header accepts no other value; bound any by the smallest block size regardless.
        default: max_block_size = 64 * 1024; break;
    }

    static constexpr UInt32 UNCOMPRESSED_BLOCK_FLAG = 0x80000000;
    static constexpr size_t BLOCK_HEADER_SIZE = 4;
    static constexpr size_t CHECKSUM_SIZE = 4;
    /// `block_size` is at most 4 MiB, so multiplying by this cannot wrap. UInt64 rather than size_t
    /// so `std::min` below deduces one type from both arguments where the two spellings differ.
    static constexpr UInt64 MAX_LZ4_BLOCK_EXPANSION = 255;

    UInt64 bound = 0;
    size_t pos = header_size;
    while (true)
    {
        if (BLOCK_HEADER_SIZE > size - pos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer ends inside an LZ4 block header");
        const auto block_header = static_cast<UInt32>(readLittleEndian<BLOCK_HEADER_SIZE>(src + pos));
        pos += BLOCK_HEADER_SIZE;
        if (block_header == 0)
            break;
        const size_t block_size = block_header & ~UNCOMPRESSED_BLOCK_FLAG;
        if (block_size > max_block_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer has an oversized LZ4 block");
        if (block_size > size - pos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer ends inside an LZ4 block");
        pos += block_size;
        if (info.blockChecksumFlag == LZ4F_blockChecksumEnabled)
        {
            if (CHECKSUM_SIZE > size - pos)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer ends inside an LZ4 block checksum");
            pos += CHECKSUM_SIZE;
        }
        /// An uncompressed block is stored verbatim, so it expands to exactly its stored bytes.
        if (block_header & UNCOMPRESSED_BLOCK_FLAG)
        {
            bound += block_size;
            continue;
        }
        /// A compressed block cannot expand past what its own stored bytes can encode either.
        bound += std::min(max_block_size, block_size * MAX_LZ4_BLOCK_EXPANSION);
    }
    if (info.contentChecksumFlag == LZ4F_contentChecksumEnabled)
    {
        if (CHECKSUM_SIZE > size - pos)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer ends inside an LZ4 content checksum");
        pos += CHECKSUM_SIZE;
    }
    /// Decompression accepts exactly one frame and no trailing bytes, so reject them here rather than
    /// bounding a payload that cannot decode and allocating for it first.
    if (pos != size)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Compressed Arrow IPC buffer has {} bytes after its LZ4 frame", size - pos);
    return bound;
}
}

FrameContentBound frameContentBound(CompressionCodec codec, const char * src, size_t size)
{
    if (codec == CompressionCodec::Zstd)
    {
        /// Both calls walk every frame of the payload, as the decompression call does, so their result
        /// stays comparable to the caller's prefix for concatenated and skippable-prefixed frames too.
        const UInt64 declared = ZSTD_findDecompressedSize(src, size);
        if (declared == ZSTD_CONTENTSIZE_ERROR)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer is not a valid ZSTD frame");
        if (declared != ZSTD_CONTENTSIZE_UNKNOWN)
            return FrameContentBound{declared, true};
        /// A frame may omit its size, and then only its block structure bounds it.
        const UInt64 bound = ZSTD_decompressBound(src, size);
        if (bound == ZSTD_CONTENTSIZE_ERROR)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer is not a valid ZSTD frame");
        return FrameContentBound{bound, false};
    }

    /// LZ4F_getFrameInfo consumes the header into the context, so the context must be fresh (it is
    /// discarded here) and cannot be one that a later decompression pass reuses.
    LZ4F_dctx * dctx = nullptr;
    if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_getVersion())))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot create LZ4 decompression context");
    LZ4F_frameInfo_t info;
    size_t header_size = size;
    const size_t ret = LZ4F_getFrameInfo(dctx, &info, src, &header_size);
    LZ4F_freeDecompressionContext(dctx);
    if (LZ4F_isError(ret))
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Compressed Arrow IPC buffer is not a valid LZ4 frame: {}", LZ4F_getErrorName(ret));

    /// A skippable frame carries no blocks and produces nothing, so it cannot expand at all.
    if (info.frameType == LZ4F_skippableFrame)
        return FrameContentBound{0, false};

    const UInt64 bound = lz4BlockOutputBound(info, src, size, header_size);
    /// `contentSize` reads 0 both when the field is absent and when it records zero, so a zero cannot
    /// be treated as a size; the blocks are then all there is to go on.
    if (info.contentSize == 0)
        return FrameContentBound{bound, false};
    /// A recorded size above what the blocks can produce describes no possible frame, and
    /// decompression rejects it, so reject it here rather than allocating for either value first.
    if (info.contentSize > bound)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Compressed Arrow IPC buffer's LZ4 frame declares {} uncompressed bytes but its blocks can "
            "produce at most {}", info.contentSize, bound);
    /// Decompression enforces a recorded size exactly, so a prefix differing from it at all is forged.
    return FrameContentBound{info.contentSize, true};
}

Compressor::~Compressor()
{
    if (zstd_ctx)
        ZSTD_freeCCtx(static_cast<ZSTD_CCtx *>(zstd_ctx));
    if (lz4_ctx)
        LZ4F_freeCompressionContext(static_cast<LZ4F_cctx *>(lz4_ctx));
}

PODArray<char> Compressor::compress(CompressionCodec codec, const char * src, size_t size, int level)
{
    PODArray<char> out;
    if (codec == CompressionCodec::Zstd)
    {
        if (!zstd_ctx)
            zstd_ctx = ZSTD_createCCtx();
        auto * cctx = static_cast<ZSTD_CCtx *>(zstd_ctx);
        const size_t bound = ZSTD_compressBound(size);
        out.resize(bound);
        const size_t n = ZSTD_compressCCtx(cctx, out.data(), bound, src, size, level);
        if (ZSTD_isError(n))
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "ZSTD compression failed: {}", ZSTD_getErrorName(n));
        out.resize(n);
        return out;
    }

    if (!lz4_ctx)
    {
        LZ4F_cctx * cctx = nullptr;
        if (LZ4F_isError(LZ4F_createCompressionContext(&cctx, LZ4F_getVersion())))
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot create LZ4 compression context");
        lz4_ctx = cctx;
    }
    auto * cctx = static_cast<LZ4F_cctx *>(lz4_ctx);

    LZ4F_preferences_t prefs;
    memset(&prefs, 0, sizeof(prefs));
    prefs.compressionLevel = level;
    /// Larger blocks mean far fewer per-block calls and headers, which dominate throughput here.
    prefs.frameInfo.blockSizeID = LZ4F_max4MB;
    prefs.frameInfo.contentSize = size;

    const size_t bound = LZ4F_compressFrameBound(size, &prefs);
    out.resize(bound);
    size_t pos = LZ4F_compressBegin(cctx, out.data(), bound, &prefs);
    if (LZ4F_isError(pos))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "LZ4 frame begin failed: {}", LZ4F_getErrorName(pos));
    const size_t body = LZ4F_compressUpdate(cctx, out.data() + pos, bound - pos, src, size, nullptr);
    if (LZ4F_isError(body))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "LZ4 frame update failed: {}", LZ4F_getErrorName(body));
    pos += body;
    const size_t end = LZ4F_compressEnd(cctx, out.data() + pos, bound - pos, nullptr);
    if (LZ4F_isError(end))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "LZ4 frame end failed: {}", LZ4F_getErrorName(end));
    pos += end;
    out.resize(pos);
    return out;
}

Decompressor::~Decompressor()
{
    if (zstd_ctx)
        ZSTD_freeDCtx(static_cast<ZSTD_DCtx *>(zstd_ctx));
    if (lz4_ctx)
        LZ4F_freeDecompressionContext(static_cast<LZ4F_dctx *>(lz4_ctx));
}

void Decompressor::decompress(
    CompressionCodec codec, const char * compressed, size_t compressed_size, char * dst, size_t uncompressed_size)
{
    if (codec == CompressionCodec::Zstd)
    {
        if (!zstd_ctx)
            zstd_ctx = ZSTD_createDCtx();
        const size_t n = ZSTD_decompressDCtx(
            static_cast<ZSTD_DCtx *>(zstd_ctx), dst, uncompressed_size, compressed, compressed_size);
        if (ZSTD_isError(n))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "ZSTD decompression failed: {}", ZSTD_getErrorName(n));
        if (n != uncompressed_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "ZSTD produced {} bytes, expected {}", n, uncompressed_size);
        return;
    }

    if (!lz4_ctx)
    {
        LZ4F_dctx * dctx = nullptr;
        if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_getVersion())))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot create LZ4 decompression context");
        lz4_ctx = dctx;
    }
    auto * dctx = static_cast<LZ4F_dctx *>(lz4_ctx);
    LZ4F_resetDecompressionContext(dctx);

    /// Decompress until LZ4F_decompress reports the frame is complete (`ret == 0`). Do not stop merely
    /// because the destination is full: the frame's end marker / checksum and any trailing bytes must
    /// still be consumed, otherwise a truncated or malformed compressed buffer that happens to expand to
    /// `uncompressed_size` bytes would be silently accepted.
    size_t dst_pos = 0;
    size_t src_pos = 0;
    size_t ret = 0;
    bool started = false;
    while (!started || ret != 0)
    {
        size_t dst_written = uncompressed_size - dst_pos;
        size_t src_consumed = compressed_size - src_pos;
        ret = LZ4F_decompress(dctx, dst + dst_pos, &dst_written, compressed + src_pos, &src_consumed, nullptr);
        if (LZ4F_isError(ret))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "LZ4 frame decompression failed: {}", LZ4F_getErrorName(ret));
        dst_pos += dst_written;
        src_pos += src_consumed;
        started = true;
        /// No forward progress while the frame is still open means the input is truncated:
        /// LZ4F_decompress wants more input but we have given it everything. Stop to avoid spinning.
        if (ret != 0 && dst_written == 0 && src_consumed == 0)
            break;
    }

    if (ret != 0)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "LZ4 frame was not fully decoded: the compressed Arrow buffer is truncated or malformed");
    if (src_pos != compressed_size)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "LZ4 frame finished with {} of {} compressed bytes left over (trailing data)",
            compressed_size - src_pos, compressed_size);
    if (dst_pos != uncompressed_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "LZ4 produced {} bytes, expected {}", dst_pos, uncompressed_size);
}

namespace
{
/// Below this much total work, the thread-pool overhead is not worth it — run inline.
constexpr size_t MIN_BYTES_FOR_PARALLEL = 1 << 20;

/// Splits [0, count) into `groups` roughly-equal contiguous ranges and runs `body(lo, hi)` for each,
/// in parallel across the shared format thread pool (or inline for a single group).
template <typename F>
void runInGroups(size_t count, size_t total_bytes, F && body)
{
    size_t groups = 1;
    if (count > 1 && total_bytes >= MIN_BYTES_FOR_PARALLEL && getFormatParsingThreadPool().isInitialized())
        groups = std::min<size_t>(count, std::max<size_t>(1, getFormatParsingThreadPool().get().getMaxThreads()));

    if (groups <= 1)
    {
        body(size_t(0), count);
        return;
    }

    const size_t per_group = (count + groups - 1) / groups;
    ThreadPoolCallbackRunnerLocal<void> runner(getFormatParsingThreadPool().get(), ThreadName::ARROW_FILE);
    for (size_t lo = 0; lo < count; lo += per_group)
    {
        const size_t hi = std::min(lo + per_group, count);
        runner.enqueueAndKeepTrack([&body, lo, hi] { body(lo, hi); });
    }
    runner.waitForAllToFinishAndRethrowFirstError();
}
}

void compressBuffersParallel(
    CompressionCodec codec, int level,
    const VectorWithMemoryTracking<std::pair<const char *, size_t>> & inputs, VectorWithMemoryTracking<PODArray<char>> & out)
{
    out.resize(inputs.size());
    size_t total_bytes = 0;
    for (const auto & in : inputs)
        total_bytes += in.second;

    runInGroups(inputs.size(), total_bytes, [&](size_t lo, size_t hi)
    {
        Compressor compressor; /// codec contexts are not thread-safe: one per group
        for (size_t i = lo; i < hi; ++i)
        {
            if (inputs[i].second > 0)
                out[i] = compressor.compress(codec, inputs[i].first, inputs[i].second, level);
        }
    });
}

void decompressBuffersParallel(CompressionCodec codec, const VectorWithMemoryTracking<DecompressJob> & jobs)
{
    size_t total_bytes = 0;
    for (const auto & job : jobs)
        total_bytes += job.dst_size;

    runInGroups(jobs.size(), total_bytes, [&](size_t lo, size_t hi)
    {
        Decompressor decompressor; /// codec contexts are not thread-safe: one per group
        for (size_t i = lo; i < hi; ++i)
        {
            /// Run the codec whenever there is a compressed payload, even when the buffer decodes to zero
            /// bytes (`dst_size == 0`): the frame's header, end marker and checksum must still be validated,
            /// otherwise a non-empty frame declaring a zero uncompressed length would have its trailing bytes
            /// accepted unchecked. A job is only ever created for `src_size > 0`, so this validates every one.
            if (jobs[i].src_size > 0)
                decompressor.decompress(codec, jobs[i].src, jobs[i].src_size, jobs[i].dst, jobs[i].dst_size);
        }
    });
}

}

#endif
