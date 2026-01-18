/// Benchmark comparing OpenZL vs ZSTD compression performance
///
/// Build: ninja openzl_benchmark (after adding to CMakeLists.txt)
/// Run:   ./openzl_benchmark

#include <iostream>
#include <iomanip>
#include <vector>
#include <random>
#include <chrono>

#include <Compression/CompressionCodecOpenZL.h>
#include <Compression/CompressionCodecZSTD.h>
#include <Common/PODArray.h>

using namespace DB;

namespace
{

struct BenchmarkResult
{
    double compression_ratio;
    double compress_speed_mb_s;
    double decompress_speed_mb_s;
};

template <typename Codec>
BenchmarkResult benchmark(Codec & codec, const char * data, size_t size, int iterations)
{
    PODArray<char> compressed(codec.getCompressedReserveSize(static_cast<UInt32>(size)));
    PODArray<char> decompressed(size + 64);

    /// Warmup
    UInt32 compressed_size = codec.compress(data, static_cast<UInt32>(size), compressed.data());

    /// Compression benchmark
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i)
        compressed_size = codec.compress(data, static_cast<UInt32>(size), compressed.data());
    auto end = std::chrono::high_resolution_clock::now();

    double compress_time = std::chrono::duration<double>(end - start).count();
    double compress_speed = (static_cast<double>(size) * iterations) / compress_time / 1e6;
    double ratio = static_cast<double>(size) / compressed_size;

    /// Decompression benchmark
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i)
        codec.decompress(compressed.data(), compressed_size, decompressed.data());
    end = std::chrono::high_resolution_clock::now();

    double decompress_time = std::chrono::duration<double>(end - start).count();
    double decompress_speed = (static_cast<double>(size) * iterations) / decompress_time / 1e6;

    return {ratio, compress_speed, decompress_speed};
}

void printResult(const std::string & name, const BenchmarkResult & result)
{
    std::cout << std::left << std::setw(12) << name
              << std::right << std::fixed << std::setprecision(2)
              << std::setw(10) << result.compression_ratio << "x"
              << std::setw(12) << result.compress_speed_mb_s << " MB/s"
              << std::setw(14) << result.decompress_speed_mb_s << " MB/s"
              << std::endl;
}

} // namespace

int main()
{
    constexpr size_t DATA_SIZE = 1000000;
    constexpr int ITERATIONS = 10;

    std::cout << "=== OpenZL vs ZSTD Benchmark ===" << std::endl;
    std::cout << "Data size: " << DATA_SIZE * sizeof(UInt64) / 1e6 << " MB" << std::endl;
    std::cout << "Iterations: " << ITERATIONS << std::endl << std::endl;

    std::cout << std::left << std::setw(12) << "Codec"
              << std::right << std::setw(11) << "Ratio"
              << std::setw(16) << "Compress"
              << std::setw(16) << "Decompress"
              << std::endl;
    std::cout << std::string(55, '-') << std::endl;

    /// Test 1: Monotonically increasing integers (ideal for OpenZL)
    {
        std::vector<UInt64> data(DATA_SIZE);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = i * 100 + (i % 17);

        const char * raw = reinterpret_cast<const char *>(data.data());
        size_t size = data.size() * sizeof(UInt64);

        std::cout << "\n[Test 1: Monotonic integers]" << std::endl;

        CompressionCodecZSTD zstd3(3);
        printResult("ZSTD(3)", benchmark(zstd3, raw, size, ITERATIONS));

        CompressionCodecOpenZL openzl;
        printResult("OpenZL", benchmark(openzl, raw, size, ITERATIONS));
    }

    /// Test 2: Random data (worst case)
    {
        std::vector<UInt64> data(DATA_SIZE);
        std::mt19937_64 rng(42);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = rng();

        const char * raw = reinterpret_cast<const char *>(data.data());
        size_t size = data.size() * sizeof(UInt64);

        std::cout << "\n[Test 2: Random data]" << std::endl;

        CompressionCodecZSTD zstd3(3);
        printResult("ZSTD(3)", benchmark(zstd3, raw, size, ITERATIONS));

        CompressionCodecOpenZL openzl;
        printResult("OpenZL", benchmark(openzl, raw, size, ITERATIONS));
    }

    /// Test 3: All zeros (highly compressible)
    {
        std::vector<UInt64> data(DATA_SIZE, 0);

        const char * raw = reinterpret_cast<const char *>(data.data());
        size_t size = data.size() * sizeof(UInt64);

        std::cout << "\n[Test 3: All zeros]" << std::endl;

        CompressionCodecZSTD zstd3(3);
        printResult("ZSTD(3)", benchmark(zstd3, raw, size, ITERATIONS));

        CompressionCodecOpenZL openzl;
        printResult("OpenZL", benchmark(openzl, raw, size, ITERATIONS));
    }

    /// Test 4: Time series simulation (timestamp-like)
    {
        std::vector<UInt64> data(DATA_SIZE);
        UInt64 ts = 1700000000;
        for (size_t i = 0; i < data.size(); ++i)
        {
            ts += 1 + (i % 10); // Small increments with some variation
            data[i] = ts;
        }

        const char * raw = reinterpret_cast<const char *>(data.data());
        size_t size = data.size() * sizeof(UInt64);

        std::cout << "\n[Test 4: Time series]" << std::endl;

        CompressionCodecZSTD zstd3(3);
        printResult("ZSTD(3)", benchmark(zstd3, raw, size, ITERATIONS));

        CompressionCodecOpenZL openzl;
        printResult("OpenZL", benchmark(openzl, raw, size, ITERATIONS));
    }

    std::cout << "\n=== Benchmark Complete ===" << std::endl;

    return 0;
}
