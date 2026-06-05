#include <Processors/Formats/Impl/ArrowIPC/BufferCompression.h>

#if USE_ARROW

#include <Common/Exception.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>
#include <algorithm>
#include <cstring>

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

    size_t dst_pos = 0;
    size_t src_pos = 0;
    while (src_pos < compressed_size && dst_pos < uncompressed_size)
    {
        size_t dst_remaining = uncompressed_size - dst_pos;
        size_t src_remaining = compressed_size - src_pos;
        const size_t ret = LZ4F_decompress(dctx, dst + dst_pos, &dst_remaining, compressed + src_pos, &src_remaining, nullptr);
        dst_pos += dst_remaining;
        src_pos += src_remaining;
        if (LZ4F_isError(ret))
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "LZ4 frame decompression failed: {}", LZ4F_getErrorName(ret));
        if (ret == 0)
            break;
    }

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
    const std::vector<std::pair<const char *, size_t>> & inputs, std::vector<PODArray<char>> & out)
{
    out.resize(inputs.size());
    size_t total_bytes = 0;
    for (const auto & in : inputs)
        total_bytes += in.second;

    runInGroups(inputs.size(), total_bytes, [&](size_t lo, size_t hi)
    {
        Compressor compressor; /// codec contexts are not thread-safe: one per group
        for (size_t i = lo; i < hi; ++i)
            if (inputs[i].second > 0)
                out[i] = compressor.compress(codec, inputs[i].first, inputs[i].second, level);
    });
}

void decompressBuffersParallel(CompressionCodec codec, const std::vector<DecompressJob> & jobs)
{
    size_t total_bytes = 0;
    for (const auto & job : jobs)
        total_bytes += job.dst_size;

    runInGroups(jobs.size(), total_bytes, [&](size_t lo, size_t hi)
    {
        Decompressor decompressor; /// codec contexts are not thread-safe: one per group
        for (size_t i = lo; i < hi; ++i)
            if (jobs[i].dst_size > 0)
                decompressor.decompress(codec, jobs[i].src, jobs[i].src_size, jobs[i].dst, jobs[i].dst_size);
    });
}

}

#endif
