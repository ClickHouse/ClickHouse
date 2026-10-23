#include <benchmark/benchmark.h>

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/copyS3File.h>
#include <IO/S3/Client.h>
#include <IO/S3/Requests.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdIStreamFromMemory.h>

#include <aws/core/utils/HashingUtils.h>

#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// The staging hash and both SDK code paths go through the AWS SDK's crypto factories, which only
/// hand out implementations after `Aws::InitAPI` has run. In production that happens inside
/// `S3::ClientFactory::instance()`.
void ensureAwsInitialized()
{
    static const bool initialized = []
    {
        S3::ClientFactory::instance();
        return true;
    }();
    (void)initialized;
}

/// A single source buffer shared by every benchmark. The staging loop copies out of it; nothing
/// mutates it.
const std::string & sourceData()
{
    static const std::string data = []
    {
        std::string result(128ull << 20, '\0');
        /// Cheap deterministic filler; MD5/CRC32/SHA256 throughput does not depend on the content.
        uint32_t state = 0x9e3779b9u;
        for (char & byte : result)
        {
            state = state * 1664525u + 1013904223u;
            byte = static_cast<char>(state >> 24);
        }
        return result;
    }();
    return data;
}

CreateReadBuffer makeSource(size_t size)
{
    const std::string & data = sourceData();
    return [&data, size]() -> std::unique_ptr<SeekableReadBuffer>
    {
        return std::make_unique<ReadBufferFromMemory>(data.data(), size);
    };
}

/// Exactly what the AWS SDK does when the checksum header is absent:
/// `AWSClient::AddContentBodyToRequest` calls `CreateMD5Implementation()->Calculate(*body)` for
/// `Content-MD5`, and the smithy interceptor's `Calculate(*body)` for `x-amz-checksum-*`. Both walk
/// the stream in `Hash::INTERNAL_HASH_STREAM_BUFFER_SIZE` (8 KiB) chunks.
std::string sdkDeriveChecksum(S3::RequestChecksum::Algorithm algorithm, Aws::IOStream & body)
{
    switch (algorithm)
    {
        case S3::RequestChecksum::Algorithm::MD5:
            return std::string(Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateMD5(body)));
        case S3::RequestChecksum::Algorithm::CRC32:
            return std::string(Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateCRC32(body)));
        case S3::RequestChecksum::Algorithm::SHA256:
            return std::string(Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateSHA256(body)));
    }
    UNREACHABLE();
}

/// The payload SHA256 the signer derives on every attempt (`AWSAuthV4Signer::ComputePayloadHash`).
/// Present in both the baseline and the new code, so it is measured only to express the saving
/// relative to the whole per-attempt cost.
std::string sdkSignerPayloadSha256(Aws::IOStream & body)
{
    return std::string(Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateSHA256(body)));
}

enum class Scenario
{
    /// Baseline: stage the part with a plain copy, then let the SDK scan the staged body for the
    /// checksum. This is what the previous code did for MD5 on every attempt, and for CRC32/SHA256
    /// once per attempt in `calculateFlexibleChecksum`.
    StageThenSdkScan,
    /// What the previous version of this change did: stage and feed the hash from the same loop.
    /// Kept only as the losing side of the comparison -- it is measurably SLOWER than hashing the
    /// finished buffer (see `BM_StagingShape`), because an `Update` interleaved with the staging
    /// reads re-reads bytes the copy just wrote while they are being evicted, and pins the hash call
    /// inside the read loop.
    FusedStagingHash,
    /// What the code does now: stage the part with a plain copy, then hash the buffer it was staged
    /// into. Two passes over the bytes, like `StageThenSdkScan`, but the second one feeds the whole
    /// part to the hash in a single `Update` straight from memory, instead of the SDK's 8 KiB stream
    /// reads through a stack buffer.
    HashStagedBuffer,
};

/// Where the hash sits relative to the copy, over the SAME buffer the part was staged into. This is
/// the byte-fair form of the comparison: both shapes copy the same bytes into the same buffer and
/// hash the same bytes, only the interleaving differs.
enum class StagingShape
{
    /// Fed right after each chunk is copied.
    Fused,
    /// The whole part is copied first, then hashed from that buffer -- the current shape.
    SplitAfterCopy,
};

/// The checksum of a part already in memory, the way the production code now computes it.
std::string memoryChecksum(S3::RequestChecksum::Algorithm algorithm, const char * data, size_t size)
{
    return S3::RequestChecksum::calculateChecksum(algorithm, data, size);
}

template <S3::RequestChecksum::Algorithm algorithm, Scenario scenario>
void BM_StageAndChecksum(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));

    for (auto _ [[maybe_unused]] : state)
    {
        std::string value;

        if constexpr (scenario == Scenario::FusedStagingHash)
        {
            /// The previous version of this change: hash fed from inside the staging loop. Needs its
            /// own copy of the loop because the production function no longer does this.
            std::string staged(size, '\0');
            const std::string & source = sourceData();
            auto hash = S3::RequestChecksum::createHash(algorithm);
            size_t done = 0;
            while (done < size)
            {
                const size_t chunk = std::min(size - done, static_cast<size_t>(1024) * 1024);
                std::memcpy(staged.data() + done, source.data() + done, chunk);
                hash->Update(reinterpret_cast<unsigned char *>(staged.data()) + done, chunk);
                done += chunk;
            }
            value = S3::RequestChecksum::base64Checksum(algorithm, *hash);
        }
        else if constexpr (scenario == Scenario::HashStagedBuffer)
        {
            /// Current production shape: `createS3UploadBody` stages the part and hashes the buffer it
            /// was staged into, in one `Update`.
            PrecalculatedUploadChecksum checksum;
            auto body = createS3UploadBody(makeSource(size), /* offset */ 0, size, algorithm, &checksum);
            value = checksum.value;
            benchmark::DoNotOptimize(body);
        }
        else
        {
            auto body = createS3UploadBody(makeSource(size), /* offset */ 0, size);
            value = sdkDeriveChecksum(algorithm, *body);
            benchmark::DoNotOptimize(body);
        }

        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// Staging alone, no checksum requested: the floor both versions share. Subtracting it from the
/// two scenarios above isolates the checksum cost itself.
void BM_StageOnly(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));

    for (auto _ [[maybe_unused]] : state)
    {
        auto body = createS3UploadBody(makeSource(size), /* offset */ 0, size);
        benchmark::DoNotOptimize(body);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// A whole upload attempt chain that the SDK retries `attempts` times. The body is staged once and
/// resent; what differs is whether the checksum has to be derived again on every attempt.
///
/// Baseline: stage once, then on each attempt scan the body for the checksum header and scan it
/// again for the signer's payload SHA256.
/// New: stage and hash once, then on each attempt only the signer's scan remains.
template <S3::RequestChecksum::Algorithm algorithm, Scenario scenario>
void BM_AttemptsWithRetries(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));
    const int attempts = static_cast<int>(state.range(1));

    for (auto _ [[maybe_unused]] : state)
    {
        std::string value;
        std::unique_ptr<StdStreamFromReadBuffer> body;

        if constexpr (scenario == Scenario::HashStagedBuffer)
        {
            PrecalculatedUploadChecksum checksum;
            body = createS3UploadBody(makeSource(size), /* offset */ 0, size, algorithm, &checksum);
            value = checksum.value;
        }
        else
        {
            body = createS3UploadBody(makeSource(size), /* offset */ 0, size);
        }

        std::string last_signed;
        for (int attempt = 0; attempt < attempts; ++attempt)
        {
            body->clear();
            body->seekg(0);

            if constexpr (scenario == Scenario::StageThenSdkScan)
                value = sdkDeriveChecksum(algorithm, *body);

            /// Every attempt re-signs the body, in both versions.
            last_signed = sdkSignerPayloadSha256(*body);
        }

        benchmark::DoNotOptimize(value);
        benchmark::DoNotOptimize(last_signed);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
    state.counters["attempts"] = attempts;
}

/// The signer's payload hash alone, over the body the upload would send. Not part of the saving;
/// it bounds it, since it stays in both versions and on every attempt.
void BM_SignerPayloadSha256(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));

    PrecalculatedUploadChecksum checksum;
    auto body = createS3UploadBody(makeSource(size), /* offset */ 0, size, S3::RequestChecksum::Algorithm::SHA256, &checksum);

    for (auto _ [[maybe_unused]] : state)
    {
        auto value = sdkSignerPayloadSha256(*body);
        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// The producer thread of `WriteBufferFromS3` stages a part by copying it into the buffer the upload
/// will read from. After the fix that copy is all the producer does, so its throughput is the plain
/// copy rate and does not depend on the selected checksum algorithm.
void BM_WbsStagingCopyOnly(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));
    const std::string & source = sourceData();

    std::string staged(size, '\0');

    for (auto _ [[maybe_unused]] : state)
    {
        std::memcpy(staged.data(), source.data(), size);
        benchmark::DoNotOptimize(staged);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// The producer thread as the fused version left it: the hash is fed from the same loop that copies
/// the part into the upload buffer, which puts every part's checksum on the single producer thread.
template <S3::RequestChecksum::Algorithm algorithm>
void BM_WbsStagingFusedHash(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));
    const std::string & source = sourceData();

    std::string staged(size, '\0');

    for (auto _ [[maybe_unused]] : state)
    {
        auto hash = S3::RequestChecksum::createHash(algorithm);
        size_t staged_bytes = 0;
        while (staged_bytes < size)
        {
            /// 1 MiB is just a copy granularity here; the hash is fed the same bytes either way.
            const size_t chunk = std::min<size_t>(size - staged_bytes, 1024 * 1024);
            std::memcpy(staged.data() + staged_bytes, source.data() + staged_bytes, chunk);
            hash->Update(reinterpret_cast<unsigned char *>(staged.data()) + staged_bytes, chunk);
            staged_bytes += chunk;
        }

        auto value = S3::RequestChecksum::base64Checksum(algorithm, *hash);
        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// The upload worker's share after the fix: the staged buffer is hashed in one `Update`. This runs on
/// the worker that sends the part, so parts are hashed in parallel.
template <S3::RequestChecksum::Algorithm algorithm>
void BM_WbsWorkerHashStagedBuffer(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));
    const std::string & source = sourceData();

    /// Staged once, outside the loop: the worker receives an already staged part.
    std::string staged(source.data(), size);

    for (auto _ [[maybe_unused]] : state)
    {
        auto value = memoryChecksum(algorithm, staged.data(), size);
        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// What the fusion replaced on the sending side: the SDK deriving the checksum from the request
/// body, once per attempt, through its 8 KiB stream buffer.
template <S3::RequestChecksum::Algorithm algorithm>
void BM_SdkDeriveFromStagedBody(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));

    std::string staged(sourceData().data(), size);
    auto body = std::make_unique<StdIStreamFromMemory>(staged.data(), size);

    for (auto _ [[maybe_unused]] : state)
    {
        body->clear();
        body->seekg(0);
        auto value = sdkDeriveChecksum(algorithm, *body);
        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
}

/// Fused vs. split hashing over the same staging buffer, for an arbitrary chunk size. Both shapes
/// copy the same bytes into the same buffer and hash the same bytes; only the interleaving differs.
/// This is the measurement that picked the current shape: split wins at every chunk size, for every
/// algorithm, on both part sizes.
template <S3::RequestChecksum::Algorithm algorithm, StagingShape shape>
void BM_StagingShape(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));
    const size_t chunk_size = static_cast<size_t>(state.range(1));
    const std::string & source = sourceData();

    std::string staged(size, '\0');

    for (auto _ [[maybe_unused]] : state)
    {
        auto hash = S3::RequestChecksum::createHash(algorithm);

        if constexpr (shape == StagingShape::Fused)
        {
            size_t done = 0;
            while (done < size)
            {
                const size_t chunk = std::min(size - done, chunk_size);
                std::memcpy(staged.data() + done, source.data() + done, chunk);
                hash->Update(reinterpret_cast<unsigned char *>(staged.data()) + done, chunk);
                done += chunk;
            }
        }
        else
        {
            size_t done = 0;
            while (done < size)
            {
                const size_t chunk = std::min(size - done, chunk_size);
                std::memcpy(staged.data() + done, source.data() + done, chunk);
                done += chunk;
            }
            done = 0;
            while (done < size)
            {
                const size_t chunk = std::min(size - done, chunk_size);
                hash->Update(reinterpret_cast<unsigned char *>(staged.data()) + done, chunk);
                done += chunk;
            }
        }

        auto value = S3::RequestChecksum::base64Checksum(algorithm, *hash);
        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
    state.counters["chunk_KiB"] = static_cast<double>(chunk_size) / 1024.0;
}

/// Sweeps the `Update` chunk size over an already staged buffer: what the upload worker pays. Small
/// chunks cost per-call overhead, large ones start streaming past L2. Finds the plateau rather than
/// assuming 1 MiB is optimal.
template <S3::RequestChecksum::Algorithm algorithm>
void BM_WorkerHashChunkSweep(benchmark::State & state)
{
    ensureAwsInitialized();
    const size_t size = static_cast<size_t>(state.range(0));
    /// `0` means "feed the whole part in a single `Update`".
    const size_t chunk_size = state.range(1) == 0 ? size : static_cast<size_t>(state.range(1));

    std::string staged(sourceData().data(), size);

    for (auto _ [[maybe_unused]] : state)
    {
        auto hash = S3::RequestChecksum::createHash(algorithm);
        size_t done = 0;
        while (done < size)
        {
            const size_t chunk = std::min(size - done, chunk_size);
            hash->Update(reinterpret_cast<unsigned char *>(staged.data()) + done, chunk);
            done += chunk;
        }
        auto value = S3::RequestChecksum::base64Checksum(algorithm, *hash);
        benchmark::DoNotOptimize(value);
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(size));
    state.counters["part_MiB"] = static_cast<double>(size) / (1024.0 * 1024.0);
    state.counters["chunk_KiB"] = static_cast<double>(chunk_size) / 1024.0;
}

void registerSizeArgs(benchmark::internal::Benchmark * benchmark)
{
    for (const int64_t part_mib : {16, 128})
        benchmark->Arg(part_mib << 20);
}

/// Part size and the number of attempts in one retry chain. The body is resent, not re-staged.
void registerRetryArgs(benchmark::internal::Benchmark * benchmark)
{
    for (const int64_t part_mib : {16, 128})
        for (const int64_t attempts : {1, 2, 3})
            benchmark->Args({part_mib << 20, attempts});
}

/// Part size and `Update` chunk size. 4 KiB is below the SDK's 8 KiB stream buffer, 64 KiB is the
/// L2 sweet spot on most server parts, 1 MiB is the production value, 16 MiB crosses into streaming
/// out of L2 for a 16 MiB part.
void registerShapeArgs(benchmark::internal::Benchmark * benchmark)
{
    for (const int64_t part_mib : {16, 128})
        for (const int64_t chunk_kib : {4, 8, 16, 64, 256, 1024, 4096})
            benchmark->Args({part_mib << 20, chunk_kib << 10});
}

/// Part size and `Update` chunk size, with `0` standing for a single whole-part `Update`.
void registerSweepArgs(benchmark::internal::Benchmark * benchmark)
{
    for (const int64_t part_mib : {16, 128})
    {
        benchmark->Args({part_mib << 20, 0});
        for (const int64_t chunk_kib : {4, 8, 16, 64, 256, 1024, 4096})
            benchmark->Args({part_mib << 20, chunk_kib << 10});
    }
}

}

#define REGISTER(name, algorithm, scenario) \
    BENCHMARK_TEMPLATE(name, S3::RequestChecksum::Algorithm::algorithm, Scenario::scenario)->Apply(registerSizeArgs)

#define REGISTER_RETRY(name, algorithm, scenario) \
    BENCHMARK_TEMPLATE(name, S3::RequestChecksum::Algorithm::algorithm, Scenario::scenario)->Apply(registerRetryArgs)

REGISTER(BM_StageAndChecksum, MD5, StageThenSdkScan);
REGISTER(BM_StageAndChecksum, MD5, FusedStagingHash);
REGISTER(BM_StageAndChecksum, MD5, HashStagedBuffer);
REGISTER(BM_StageAndChecksum, CRC32, StageThenSdkScan);
REGISTER(BM_StageAndChecksum, CRC32, FusedStagingHash);
REGISTER(BM_StageAndChecksum, CRC32, HashStagedBuffer);
REGISTER(BM_StageAndChecksum, SHA256, StageThenSdkScan);
REGISTER(BM_StageAndChecksum, SHA256, FusedStagingHash);
REGISTER(BM_StageAndChecksum, SHA256, HashStagedBuffer);

REGISTER_RETRY(BM_AttemptsWithRetries, MD5, StageThenSdkScan);
REGISTER_RETRY(BM_AttemptsWithRetries, MD5, HashStagedBuffer);
REGISTER_RETRY(BM_AttemptsWithRetries, CRC32, StageThenSdkScan);
REGISTER_RETRY(BM_AttemptsWithRetries, CRC32, HashStagedBuffer);
REGISTER_RETRY(BM_AttemptsWithRetries, SHA256, StageThenSdkScan);
REGISTER_RETRY(BM_AttemptsWithRetries, SHA256, HashStagedBuffer);

BENCHMARK(BM_StageOnly)->Apply(registerSizeArgs);
BENCHMARK(BM_SignerPayloadSha256)->Apply(registerSizeArgs);

/// The `WriteBufferFromS3` split, measured on its own: what the producer thread pays once it no
/// longer hashes, what it used to pay, and what the upload worker pays per part.
BENCHMARK(BM_WbsStagingCopyOnly)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_WbsStagingFusedHash, S3::RequestChecksum::Algorithm::MD5)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_WbsStagingFusedHash, S3::RequestChecksum::Algorithm::CRC32)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_WbsStagingFusedHash, S3::RequestChecksum::Algorithm::SHA256)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_WbsWorkerHashStagedBuffer, S3::RequestChecksum::Algorithm::MD5)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_WbsWorkerHashStagedBuffer, S3::RequestChecksum::Algorithm::CRC32)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_WbsWorkerHashStagedBuffer, S3::RequestChecksum::Algorithm::SHA256)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_SdkDeriveFromStagedBody, S3::RequestChecksum::Algorithm::MD5)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_SdkDeriveFromStagedBody, S3::RequestChecksum::Algorithm::CRC32)->Apply(registerSizeArgs);
BENCHMARK_TEMPLATE(BM_SdkDeriveFromStagedBody, S3::RequestChecksum::Algorithm::SHA256)->Apply(registerSizeArgs);

/// Fused vs. split over the same buffer: isolates what interleaving buys, with the chunk size as a
/// variable instead of a constant.
#define REGISTER_SHAPE(name, algorithm, shape) \
    BENCHMARK_TEMPLATE(name, S3::RequestChecksum::Algorithm::algorithm, StagingShape::shape)->Apply(registerShapeArgs)

REGISTER_SHAPE(BM_StagingShape, MD5, Fused);
REGISTER_SHAPE(BM_StagingShape, MD5, SplitAfterCopy);
REGISTER_SHAPE(BM_StagingShape, CRC32, Fused);
REGISTER_SHAPE(BM_StagingShape, CRC32, SplitAfterCopy);
REGISTER_SHAPE(BM_StagingShape, SHA256, Fused);
REGISTER_SHAPE(BM_StagingShape, SHA256, SplitAfterCopy);

BENCHMARK_TEMPLATE(BM_WorkerHashChunkSweep, S3::RequestChecksum::Algorithm::MD5)->Apply(registerSweepArgs);
BENCHMARK_TEMPLATE(BM_WorkerHashChunkSweep, S3::RequestChecksum::Algorithm::CRC32)->Apply(registerSweepArgs);
BENCHMARK_TEMPLATE(BM_WorkerHashChunkSweep, S3::RequestChecksum::Algorithm::SHA256)->Apply(registerSweepArgs);

#endif
