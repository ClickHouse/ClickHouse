#include <gtest/gtest.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Common/Exception.h>

#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{
    /// Helper: round-trip serialize+deserialize and return the output bitmap.
    std::unique_ptr<DeleteBitmap> roundtrip(const DeleteBitmap & in)
    {
        String buf;
        {
            WriteBufferFromString out(buf);
            in.serialize(out);
        }
        ReadBufferFromString rb(buf);
        return DeleteBitmap::deserialize(rb);
    }
}

/// ---------- primitive ----------
///
/// One smoke test gates the wrapper layer; the rest of the suite exercises
/// our own logic: `containsBulk`, `rangeCardinality`, the dynamic-width
/// upgrade, serialize/deserialize invariants, and filename helpers.

TEST(DeleteBitmapTest, WrapperSmoke)
{
    DeleteBitmap a;
    EXPECT_TRUE(a.empty());
    EXPECT_EQ(a.cardinality(), 0u);
    EXPECT_GT(a.memoryUsage(), 0u);

    a.add(10);
    a.add(20);
    a.add(10); /// idempotent
    a.addMany({30, 40});
    EXPECT_FALSE(a.empty());
    EXPECT_EQ(a.cardinality(), 4u);
    EXPECT_TRUE(a.contains(10));
    EXPECT_FALSE(a.contains(15));

    DeleteBitmap b;
    b.add(20);
    b.add(50);
    a.merge(b);
    EXPECT_EQ(a.cardinality(), 5u);
    EXPECT_TRUE(a.contains(50));

    auto v = a.toVector();
    EXPECT_EQ(v, (std::vector<UInt64>{10, 20, 30, 40, 50}));
}

TEST(DeleteBitmapTest, ContainsBulkMatchesScalar)
{
    /// Correctness lock for the bulk API: must produce the same keep/drop
    /// decision as per-row `contains`. Exercises empty-bitmap fast path,
    /// n==0, sparse + dense shapes, and offsets inside + outside the
    /// bitmap.
    DeleteBitmap bm;
    /// Mixed shape: singletons + a dense run — crosses the CRoaring
    /// container-type boundary (array vs bitset), so containsBulk must
    /// handle both paths in a single call.
    for (UInt32 i = 0; i < 5000; ++i)
        bm.add(100 + i);
    bm.add(1'000'000);
    bm.add(2'000'000);
    bm.add(std::numeric_limits<UInt32>::max());

    /// n == 0 is a no-op.
    {
        uint8_t dummy = 0xAA;
        bm.containsBulk(nullptr, 0, &dummy);
        EXPECT_EQ(dummy, 0xAA);
    }

    /// Empty bitmap → all 1s.
    {
        DeleteBitmap empty;
        std::vector<UInt64> rows = {0, 1, 999, std::numeric_limits<UInt32>::max()};
        std::vector<uint8_t> keep(rows.size(), 0);
        empty.containsBulk(rows.data(), rows.size(), keep.data());
        for (auto k : keep)
            EXPECT_EQ(k, 1u);
    }

    /// Mixed hit / miss pattern. Final probe is above UInt32::max — the
    /// narrow path must short-circuit it as "keep".
    constexpr UInt64 kU32Max = std::numeric_limits<UInt32>::max();
    std::vector<UInt64> rows = {
        0, 50, 99, 100, 500, 5099, 5100, 1'000'000, 1'000'001,
        2'000'000, 4'000'000, kU32Max, kU32Max + 1};
    std::vector<uint8_t> keep(rows.size(), 0xFF);
    bm.containsBulk(rows.data(), rows.size(), keep.data());
    for (size_t i = 0; i < rows.size(); ++i)
    {
        const uint8_t expected = bm.contains(rows[i]) ? 0 : 1;
        EXPECT_EQ(keep[i], expected)
            << "row " << rows[i] << " idx " << i
            << " bulk=" << int(keep[i]) << " scalar=" << int(expected);
    }

    /// Randomised cross-check: 10k random probes against a sparse bitmap.
    std::mt19937 rng(424242); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<UInt32> dist(0, 10'000'000);
    std::vector<UInt64> many(10'000);
    for (auto & r : many)
        r = dist(rng);
    std::vector<uint8_t> bulk_keep(many.size(), 0);
    bm.containsBulk(many.data(), many.size(), bulk_keep.data());
    for (size_t i = 0; i < many.size(); ++i)
    {
        const uint8_t expected = bm.contains(many[i]) ? 0 : 1;
        EXPECT_EQ(bulk_keep[i], expected) << "random idx " << i << " row " << many[i];
    }
}

TEST(DeleteBitmapTest, RangeCardinality)
{
    DeleteBitmap bm;
    bm.add(0);
    bm.add(5);
    bm.add(10);
    bm.add(99);

    /// Empty bitmap → 0 over any range.
    DeleteBitmap empty;
    EXPECT_EQ(empty.rangeCardinality(0, 1000), 0u);

    /// Inverted / zero-length ranges.
    EXPECT_EQ(bm.rangeCardinality(10, 10), 0u);
    EXPECT_EQ(bm.rangeCardinality(20, 5), 0u);

    /// Half-open semantics.
    EXPECT_EQ(bm.rangeCardinality(0, 1), 1u);   /// contains 0
    EXPECT_EQ(bm.rangeCardinality(0, 6), 2u);   /// contains 0, 5
    EXPECT_EQ(bm.rangeCardinality(5, 11), 2u);  /// contains 5, 10
    EXPECT_EQ(bm.rangeCardinality(6, 10), 0u);  /// nothing
    EXPECT_EQ(bm.rangeCardinality(6, 11), 1u);  /// contains 10
    EXPECT_EQ(bm.rangeCardinality(0, 100), 4u); /// all

    /// Range that extends past set elements.
    EXPECT_EQ(bm.rangeCardinality(50, 1000), 1u); /// contains 99
}

TEST(DeleteBitmapTest, RangeCardinalityRangeAboveUInt32MaxIsZeroInNarrowMode)
{
    /// In narrow mode, any range portion above `UInt32::max` cannot intersect.
    DeleteBitmap bm;
    bm.add(std::numeric_limits<UInt32>::max());
    EXPECT_EQ(bm.cardinality(), 1u);

    constexpr UInt64 kMax = std::numeric_limits<UInt32>::max();
    EXPECT_EQ(bm.rangeCardinality(kMax + 1, kMax + 2), 0u);
    EXPECT_EQ(bm.rangeCardinality(kMax + 100, kMax + 200), 0u);

    /// Range straddling the ceiling still includes row `UInt32::max`.
    EXPECT_EQ(bm.rangeCardinality(kMax, kMax + 100), 1u);
}

/// ---------- 32 → 64-bit auto-upgrade ----------

TEST(DeleteBitmapTest, AutoUpgradeOnAdd)
{
    DeleteBitmap bm;
    bm.add(7);
    bm.add(std::numeric_limits<UInt32>::max());

    constexpr UInt64 kAboveU32 = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;
    bm.add(kAboveU32);

    /// `contains(kAboveU32)` is the upgrade-trigger lock: if `add` failed to
    /// upgrade, the 32-bit path would truncate to `add(0)` and this would be
    /// false (while `contains(0)` would be unexpectedly true).
    EXPECT_TRUE(bm.contains(7));
    EXPECT_TRUE(bm.contains(std::numeric_limits<UInt32>::max()));
    EXPECT_TRUE(bm.contains(kAboveU32));
    EXPECT_FALSE(bm.contains(0));
    EXPECT_EQ(bm.cardinality(), 3u);

    /// Range query crosses the UInt32 ceiling without clamping after upgrade.
    EXPECT_EQ(bm.rangeCardinality(0, kAboveU32 + 1), 3u);
    EXPECT_EQ(bm.rangeCardinality(kAboveU32, kAboveU32 + 1), 1u);
}

TEST(DeleteBitmapTest, AutoUpgradeOnAddMany)
{
    DeleteBitmap bm;
    constexpr UInt64 kAboveU32 = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 5;
    bm.addMany({1, 2, 3, kAboveU32});
    EXPECT_EQ(bm.cardinality(), 4u);
    /// Locks the upgrade: without it, addMany would narrow-cast kAboveU32
    /// to a small UInt32 value, leaving `contains(kAboveU32)` false.
    EXPECT_TRUE(bm.contains(kAboveU32));
    EXPECT_FALSE(bm.contains(static_cast<UInt64>(kAboveU32) & 0xFFFF'FFFFu));

    /// All-narrow input still works.
    DeleteBitmap narrow;
    narrow.addMany({10, 20, 30});
    EXPECT_EQ(narrow.cardinality(), 3u);
    EXPECT_TRUE(narrow.contains(20));
}

TEST(DeleteBitmapTest, MergeNarrowInto64BitUpgrades)
{
    constexpr UInt64 kWideValue = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 10;

    DeleteBitmap b64;
    b64.add(kWideValue);

    DeleteBitmap narrow;
    narrow.add(5);

    narrow.merge(b64);
    EXPECT_TRUE(narrow.contains(5));
    /// Locks the merge-upgrade: if narrow stayed 32-bit, the wide value
    /// could not be represented and `contains` would return false.
    EXPECT_TRUE(narrow.contains(kWideValue));
}

TEST(DeleteBitmapTest, ToVectorReturnsUInt64)
{
    DeleteBitmap bm;
    constexpr UInt64 kAboveU32 = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;
    bm.addMany({1, kAboveU32, 7});

    auto v = bm.toVector();
    /// Element ordering + UInt64-typed result locks the 64-bit path: a
    /// 32-bit `toUint32Array` could not produce `kAboveU32` at all.
    EXPECT_EQ(v, (std::vector<UInt64>{1, 7, kAboveU32}));
}

/// ---------- serialization ----------

TEST(DeleteBitmapTest, RoundtripEmpty)
{
    DeleteBitmap in;
    auto out = roundtrip(in);
    ASSERT_NE(out, nullptr);
    EXPECT_TRUE(out->empty());
    EXPECT_EQ(out->cardinality(), 0u);
}

TEST(DeleteBitmapTest, RoundtripSingleRow)
{
    DeleteBitmap in;
    in.add(12345);
    auto out = roundtrip(in);
    EXPECT_EQ(out->cardinality(), 1u);
    EXPECT_TRUE(out->contains(12345));
    EXPECT_FALSE(out->contains(12346));
}

TEST(DeleteBitmapTest, RoundtripSparse1M)
{
    DeleteBitmap in;
    std::mt19937 rng(12345); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<UInt32> dist(0, 100'000'000);
    std::vector<UInt64> samples;
    samples.reserve(1'000'000);
    for (size_t i = 0; i < 1'000'000; ++i)
        samples.push_back(dist(rng));
    in.addMany(samples);

    auto out = roundtrip(in);
    EXPECT_EQ(out->cardinality(), in.cardinality());
    /// Spot-check 1000 random samples.
    std::mt19937 rng2(9999); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<size_t> idx_dist(0, samples.size() - 1);
    for (size_t i = 0; i < 1000; ++i)
    {
        UInt64 v = samples[idx_dist(rng2)];
        EXPECT_TRUE(out->contains(v));
    }
}

TEST(DeleteBitmapTest, RoundtripDense1M)
{
    DeleteBitmap in;
    /// A contiguous dense range — roaring uses its run-length container here.
    std::vector<UInt64> samples;
    samples.reserve(1'000'000);
    for (UInt32 i = 0; i < 1'000'000; ++i)
        samples.push_back(i);
    in.addMany(samples);
    auto out = roundtrip(in);
    EXPECT_EQ(out->cardinality(), 1'000'000u);
    EXPECT_TRUE(out->contains(0));
    EXPECT_TRUE(out->contains(999'999));
    EXPECT_FALSE(out->contains(1'000'000));
}

TEST(DeleteBitmapTest, Roundtrip64Bit)
{
    /// Auto-upgraded 64-bit bitmap must survive a round-trip.
    DeleteBitmap in;
    constexpr UInt64 base = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;
    in.addMany({0, 1, base, base + 100, base + 1'000'000});

    auto out = roundtrip(in);
    EXPECT_EQ(out->cardinality(), 5u);
    EXPECT_TRUE(out->contains(0));
    /// Containment of a value above UInt32::max can only be true if the
    /// round-trip preserved the 64-bit representation (32-bit deserialize
    /// would have either truncated or rejected the payload).
    EXPECT_TRUE(out->contains(base));
    EXPECT_TRUE(out->contains(base + 1'000'000));
    EXPECT_FALSE(out->contains(base + 1));
}

TEST(DeleteBitmapTest, CRCMismatchRejected)
{
    DeleteBitmap in;
    for (UInt32 i = 0; i < 10; ++i)
        in.add(i);

    String buf;
    {
        WriteBufferFromString out(buf);
        in.serialize(out);
    }
    /// Flip a byte in the middle (inside the roaring body).
    ASSERT_GT(buf.size(), 16u);
    buf[buf.size() / 2] ^= 0x5A;

    ReadBufferFromString rb(buf);
    EXPECT_ANY_THROW({ auto _ = DeleteBitmap::deserialize(rb); });
}

TEST(DeleteBitmapTest, TrailingCRCTamperingRejected)
{
    DeleteBitmap in;
    in.add(42);
    String buf;
    {
        WriteBufferFromString out(buf);
        in.serialize(out);
    }
    /// Flip a bit in the stored CRC (final 4 bytes).
    buf[buf.size() - 1] ^= 0x01;
    ReadBufferFromString rb(buf);
    EXPECT_ANY_THROW({ auto _ = DeleteBitmap::deserialize(rb); });
}

TEST(DeleteBitmapTest, VersionMismatchRejected)
{
    DeleteBitmap in;
    in.add(5);
    String buf;
    {
        WriteBufferFromString out(buf);
        in.serialize(out);
    }
    /// Bump the LE-encoded version field (bytes [4..8]) to a value outside
    /// {VERSION_R32, VERSION_R64}. Use explicit LE byte writes — host-native
    /// memcpy would poke the wrong byte on big-endian builds (e.g. s390x).
    buf[sizeof(UInt32) + 0] = 99;
    buf[sizeof(UInt32) + 1] = 0;
    buf[sizeof(UInt32) + 2] = 0;
    buf[sizeof(UInt32) + 3] = 0;
    /// CRC will also mismatch now, but the version check fires first — either
    /// exception is acceptable; we just require that deserialization doesn't
    /// silently succeed with a bitmap we never wrote.
    ReadBufferFromString rb(buf);
    EXPECT_ANY_THROW({ auto _ = DeleteBitmap::deserialize(rb); });
}

TEST(DeleteBitmapTest, MagicMismatchRejected)
{
    DeleteBitmap in;
    String buf;
    {
        WriteBufferFromString out(buf);
        in.serialize(out);
    }
    buf[0] = 'X';
    ReadBufferFromString rb(buf);
    EXPECT_ANY_THROW({ auto _ = DeleteBitmap::deserialize(rb); });
}

TEST(DeleteBitmapTest, OversizedDeclaredBodyRejectedBeforeAllocation)
{
    DeleteBitmap in;
    String buf;
    {
        WriteBufferFromString out(buf);
        in.serialize(out);
    }

    /// `body_size` is the third LE-encoded field. All-0xFF is endian-symmetric
    /// (every byte 0xFF) so an explicit byte poke is portable.
    buf[sizeof(UInt32) * 2 + 0] = static_cast<char>(0xFF);
    buf[sizeof(UInt32) * 2 + 1] = static_cast<char>(0xFF);
    buf[sizeof(UInt32) * 2 + 2] = static_cast<char>(0xFF);
    buf[sizeof(UInt32) * 2 + 3] = static_cast<char>(0xFF);

    ReadBufferFromString rb(buf);
    try
    {
        auto _ = DeleteBitmap::deserialize(rb);
        FAIL() << "Expected oversized DeleteBitmap payload to be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA);
    }
}

TEST(DeleteBitmapTest, TrailingBytesAfterCRCRejected)
{
    DeleteBitmap in;
    in.add(7);
    String buf;
    {
        WriteBufferFromString out(buf);
        in.serialize(out);
    }
    /// Appending junk bytes after the CRC must be rejected via the
    /// `!in.eof()` check; otherwise torn copies / accidental appends look valid.
    buf.append("\xDE\xAD\xBE\xEF", 4);
    ReadBufferFromString rb(buf);
    try
    {
        auto _ = DeleteBitmap::deserialize(rb);
        FAIL() << "Expected trailing bytes after CRC to be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA);
    }
}

TEST(DeleteBitmapTest, FileNameRoundtrip)
{
    EXPECT_EQ(DeleteBitmap::fileNameForBlockNumber(0), "delete_bitmap_0.rbm");
    EXPECT_EQ(DeleteBitmap::fileNameForBlockNumber(12345), "delete_bitmap_12345.rbm");

    EXPECT_TRUE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_0.rbm"));
    EXPECT_TRUE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_999.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_abc.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("foo.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_1"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_1.rbm.tmp"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile(""));
    /// Noncanonical numeric forms must be rejected so two filenames cannot
    /// resolve to the same block number (would confuse `readBitmapFromStorage`).
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_+7.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_-7.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_007.rbm"));
    EXPECT_FALSE(DeleteBitmap::isDeleteBitmapFile("delete_bitmap_ 7.rbm"));

    EXPECT_EQ(DeleteBitmap::parseBlockNumberFromFileName("delete_bitmap_0.rbm"), 0U);
    EXPECT_EQ(DeleteBitmap::parseBlockNumberFromFileName("delete_bitmap_999.rbm"), 999U);
    EXPECT_THROW(DeleteBitmap::parseBlockNumberFromFileName("foo.rbm"), Exception);
}

/// ---------- microbenchmarks (informational, gated behind DISABLED_ prefix) ----------
///
/// These are not correctness tests — they measure serialize / deserialize /
/// contains cost on 1 M-entry bitmaps. Run with
///   ./build/src/unit_tests_dbms --gtest_also_run_disabled_tests --gtest_filter='DeleteBitmapBench*'
///
/// Reference numbers (Graviton-class AArch64,
///   armv8.2-a+simd+crypto+dotprod+ssbs+rcpc+bf16,
///   ClickHouse release build, 10-iteration averages):
///
///   shape               serialize   deserialize   contains (10 M probes)   wire size
///   ---------------------------------------------------------------------------------
///   Sparse  1 M random    1054 µs      1129 µs          117 ns/call          ~2.0 MB
///   Dense   1 M [0,1e6)     66 µs        63 µs           25 ns/call          ~128 KB
///
/// These are informational anchors for re-runs on different hardware — the
/// gates are not enforced in CI.
///
/// ContainsBulk vs scalar contains on the same shapes (Graviton-class
/// AArch64, release build, 10 M probes, median of 3 runs; bench body
/// in `DISABLED_ContainsBulkVsScalar` at the end of this file):
///
///   shape       scalar ns/call   bulk ns/call   speedup
///   ----------------------------------------------------
///   Sparse 1 M      98.4 ns         92.1 ns      1.06x
///   Dense  1 M      13.6 ns         10.2 ns      1.34x
///
/// The speedup on the dense-bitmap shape comes from roaring's
/// `BulkContext` keeping the container pointer cached across
/// consecutive probes in the same upper-16 bin. The sparse shape has
/// most probes hitting different bins, so the context caches a miss
/// that doesn't help — the 6% residual is just the amortised vtable
/// dispatch.

/// NOLINTBEGIN(cert-msc32-c, cert-msc51-cpp, modernize-use-std-print)
TEST(DeleteBitmapBench, DISABLED_Sparse1M)
{
    std::vector<UInt64> samples;
    {
        std::mt19937 rng(1);
        std::uniform_int_distribution<UInt32> d(0, 100'000'000);
        samples.reserve(1'000'000);
        for (size_t i = 0; i < 1'000'000; ++i) samples.push_back(d(rng));
    }
    DeleteBitmap bm;
    bm.addMany(samples);
    std::printf("[bench] sparse 1M cardinality=%zu\n", bm.cardinality());

    constexpr int N = 10;

    String buf;
    double ser_us = 0;
    for (int i = 0; i < N; ++i)
    {
        buf.clear();
        WriteBufferFromString wb(buf);
        auto t0 = std::chrono::steady_clock::now();
        bm.serialize(wb);
        wb.finalize();
        auto t1 = std::chrono::steady_clock::now();
        ser_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
    }
    std::printf("[bench] sparse 1M serialize: %.1f us avg, %zu bytes on wire\n", ser_us / N, buf.size());

    double deser_us = 0;
    for (int i = 0; i < N; ++i)
    {
        ReadBufferFromString rb(buf);
        auto t0 = std::chrono::steady_clock::now();
        auto loaded = DeleteBitmap::deserialize(rb);
        auto t1 = std::chrono::steady_clock::now();
        deser_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
        ASSERT_NE(loaded, nullptr);
    }
    std::printf("[bench] sparse 1M deserialize: %.1f us avg\n", deser_us / N);

    {
        std::mt19937 rng2(42);
        std::uniform_int_distribution<UInt32> d2(0, 100'000'000);
        constexpr size_t PROBES = 10'000'000;
        auto t0 = std::chrono::steady_clock::now();
        size_t hits = 0;
        for (size_t i = 0; i < PROBES; ++i)
            if (bm.contains(d2(rng2))) ++hits;
        auto t1 = std::chrono::steady_clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        std::printf("[bench] sparse 1M contains: %zu probes in %.1f ms, %.1f ns/call, hits=%zu\n",
                    PROBES, us / 1000.0, us * 1000.0 / PROBES, hits);
    }
}

TEST(DeleteBitmapBench, DISABLED_Dense1M)
{
    std::vector<UInt64> samples;
    samples.reserve(1'000'000);
    for (UInt32 i = 0; i < 1'000'000; ++i) samples.push_back(i);
    DeleteBitmap bm;
    bm.addMany(samples);

    constexpr int N = 10;
    String buf;
    double ser_us = 0;
    for (int i = 0; i < N; ++i)
    {
        buf.clear();
        WriteBufferFromString wb(buf);
        auto t0 = std::chrono::steady_clock::now();
        bm.serialize(wb);
        wb.finalize();
        auto t1 = std::chrono::steady_clock::now();
        ser_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
    }
    std::printf("[bench] dense 1M serialize: %.1f us avg, %zu bytes on wire\n", ser_us / N, buf.size());

    double deser_us = 0;
    for (int i = 0; i < N; ++i)
    {
        ReadBufferFromString rb(buf);
        auto t0 = std::chrono::steady_clock::now();
        auto loaded = DeleteBitmap::deserialize(rb);
        auto t1 = std::chrono::steady_clock::now();
        deser_us += std::chrono::duration<double, std::micro>(t1 - t0).count();
        ASSERT_NE(loaded, nullptr);
    }
    std::printf("[bench] dense 1M deserialize: %.1f us avg\n", deser_us / N);

    {
        std::mt19937 rng2(42);
        std::uniform_int_distribution<UInt32> d2(0, 2'000'000);
        constexpr size_t PROBES = 10'000'000;
        auto t0 = std::chrono::steady_clock::now();
        size_t hits = 0;
        for (size_t i = 0; i < PROBES; ++i)
            if (bm.contains(d2(rng2))) ++hits;
        auto t1 = std::chrono::steady_clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        std::printf("[bench] dense 1M contains: %zu probes in %.1f ms, %.1f ns/call, hits=%zu\n",
                    PROBES, us / 1000.0, us * 1000.0 / PROBES, hits);
    }
}

/// Bulk-vs-scalar containment benchmark. Probes 10 M precomputed row
/// offsets (materialised up-front so the benchmark does not charge RNG
/// cost to either path) against sparse-1M and dense-1M bitmaps. Reports
/// ns/call for each path and the speedup ratio.
TEST(DeleteBitmapBench, DISABLED_ContainsBulkVsScalar)
{
    auto bench_one = [](const char * label, const DeleteBitmap & bm, UInt32 probe_max) {
        std::mt19937 rng(0xC0FFEE);
        std::uniform_int_distribution<UInt32> dist(0, probe_max);
        constexpr size_t PROBES = 10'000'000;
        std::vector<UInt64> probes(PROBES);
        for (auto & p : probes)
            p = dist(rng);

        /// Scalar loop. Accumulate into a local and assign to a volatile at
        /// the end so the optimiser can't eliminate the loop but the
        /// per-iteration work is not inflated by volatile semantics.
        size_t scalar_drops = 0;
        auto s0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < PROBES; ++i)
            if (bm.contains(probes[i])) ++scalar_drops;
        auto s1 = std::chrono::steady_clock::now();
        [[maybe_unused]] volatile size_t scalar_drops_sink = scalar_drops;
        double scalar_us = std::chrono::duration<double, std::micro>(s1 - s0).count();

        /// Bulk API into a pre-sized keep buffer.
        std::vector<uint8_t> keep(PROBES, 0);
        auto b0 = std::chrono::steady_clock::now();
        bm.containsBulk(probes.data(), probes.size(), keep.data());
        auto b1 = std::chrono::steady_clock::now();
        double bulk_us = std::chrono::duration<double, std::micro>(b1 - b0).count();

        /// Sanity check equivalence on a small sample so a perf regression
        /// can't silently land a correctness regression.
        size_t mismatches = 0;
        for (size_t i = 0; i < std::min<size_t>(1000, PROBES); ++i)
        {
            const uint8_t expected = bm.contains(probes[i]) ? 0 : 1;
            if (keep[i] != expected) ++mismatches;
        }
        ASSERT_EQ(mismatches, 0u);

        const double scalar_ns = scalar_us * 1000.0 / PROBES;
        const double bulk_ns = bulk_us * 1000.0 / PROBES;
        std::printf("[bench] %s scalar: %.1f ns/call (%.1f ms total, drops=%zu)\n",
                    label, scalar_ns, scalar_us / 1000.0, scalar_drops);
        std::printf("[bench] %s bulk:   %.1f ns/call (%.1f ms total)\n",
                    label, bulk_ns, bulk_us / 1000.0);
        std::printf("[bench] %s speedup: %.2fx\n", label, scalar_ns / bulk_ns);
    };

    /// Sparse 1M — random in [0, 1e8].
    {
        DeleteBitmap bm;
        std::mt19937 rng(1);
        std::uniform_int_distribution<UInt32> d(0, 100'000'000);
        std::vector<UInt64> samples(1'000'000);
        for (auto & s : samples) s = d(rng);
        bm.addMany(samples);
        bench_one("sparse 1M", bm, 100'000'000);
    }

    /// Dense 1M — contiguous [0, 1e6).
    {
        DeleteBitmap bm;
        std::vector<UInt64> samples(1'000'000);
        for (UInt32 i = 0; i < 1'000'000; ++i) samples[i] = i;
        bm.addMany(samples);
        bench_one("dense 1M", bm, 2'000'000);
    }
}
/// NOLINTEND(cert-msc32-c, cert-msc51-cpp, modernize-use-std-print)
