#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/HashCombine32.h>
#include <Common/thread_local_rng.h>

#include <cstring>
#include <vector>

using namespace DB;

namespace
{
/// Fixed seed: the distributional tests assert a p=0.001 chi-square cutoff, which a correct
/// hash exceeds with non-zero probability. A deterministic seed keeps those checks (and every
/// other randomized case here) reproducible so they cannot flake in CI.
pcg64 rng(0x9e3779b97f4a7c15ULL); // NOLINT(cert-err58-cpp,bugprone-throwing-static-initialization)

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

ColumnUInt32::MutablePtr makeUInt32Column(const std::vector<UInt32> & vals)
{
    auto col = ColumnUInt32::create();
    for (auto v : vals)
        col->insert(static_cast<UInt64>(v));
    return col;
}

ColumnString::MutablePtr makeStringColumn(const std::vector<std::string> & vals)
{
    auto col = ColumnString::create();
    for (const auto & s : vals)
        col->insertData(s.data(), s.size());
    return col;
}

std::vector<UInt32> randomUInts(size_t n)
{
    std::vector<UInt32> v(n);
    for (auto & x : v)
        x = static_cast<UInt32>(rng());
    return v;
}

/// Build a ColumnArray(UInt32) with random row lengths in [0, max_len].
ColumnArray::MutablePtr makeUInt32ArrayColumn(size_t num_rows, size_t max_len)
{
    auto data = ColumnUInt32::create();
    auto offsets = ColumnUInt64::create();
    size_t acc = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        const size_t len = rng() % (max_len + 1);
        for (size_t j = 0; j < len; ++j)
            data->insert(static_cast<UInt64>(rng()));
        acc += len;
        offsets->insert(static_cast<UInt64>(acc));
    }
    return ColumnArray::create(std::move(data), std::move(offsets));
}

/// Verify that splitting the row range in two produces the same per-row hashes
/// as a single call over the whole range.
void expectRangeSplitConsistent(const IColumn & col)
{
    const size_t n = col.size();
    std::vector<uint32_t> full(n);
    std::vector<uint32_t> split(n);

    col.computeHashInto(0, n, full.data(), true);

    const size_t mid = n / 2;
    col.computeHashInto(0, mid, split.data(), true);
    col.computeHashInto(mid, n, split.data() + mid, true);

    EXPECT_EQ(full, split);
}
}


// ──────────────────────────────────────────────────────────────────────
// 1. Row-range independence: splitting into two halves gives the same
//    result as one call over the full range.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, RowRangePartitionUInt32)
{
    const size_t n = 1024;
    auto vals = randomUInts(n);
    auto col = makeUInt32Column(vals);

    std::vector<uint32_t> full(n);
    std::vector<uint32_t> split(n);

    col->computeHashInto(0, n, full.data(), true);

    const size_t mid = n / 2;
    col->computeHashInto(0, mid, split.data(), true);
    col->computeHashInto(mid, n, split.data() + mid, true);

    EXPECT_EQ(full, split);
}


// ──────────────────────────────────────────────────────────────────────
// 2. initial=false accumulates across columns deterministically.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, MultiColumnCompositionDeterministic)
{
    const size_t n = 512;
    auto col0 = makeUInt32Column(randomUInts(n));
    auto col1 = makeUInt32Column(randomUInts(n));

    std::vector<uint32_t> a(n);
    std::vector<uint32_t> b(n);

    for (int rep = 0; rep < 20; ++rep)
    {
        col0->computeHashInto(0, n, a.data(), true);
        col1->computeHashInto(0, n, a.data(), false);

        col0->computeHashInto(0, n, b.data(), true);
        col1->computeHashInto(0, n, b.data(), false);

        EXPECT_EQ(a, b) << "Mismatch on rep " << rep;
    }
}


// ──────────────────────────────────────────────────────────────────────
// 3. initial=true overwrites; initial=false combines — manual check.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, InitialFlagSemantics)
{
    const size_t n = 16;
    auto col = makeUInt32Column(randomUInts(n));

    std::vector<uint32_t> out_init(n, 0xDEADBEEFU);
    col->computeHashInto(0, n, out_init.data(), true);

    // Any initial value should be ignored when initial==true.
    std::vector<uint32_t> out_zero(n, 0U);
    col->computeHashInto(0, n, out_zero.data(), true);

    EXPECT_EQ(out_init, out_zero);

    // initial==false must combine with the existing value.
    // Use fmix32Combined to match what computeHashInto(initial=false) produces:
    // re-hashing the same column into itself is fmix32Combined(hash, hash).
    std::vector<uint32_t> combined_manual(n);
    for (size_t i = 0; i < n; ++i)
        combined_manual[i] = fmix32Combined(out_init[i], out_init[i]);

    std::vector<uint32_t> combined_api = out_init;
    col->computeHashInto(0, n, combined_api.data(), false);

    EXPECT_NE(combined_api, out_init) << "initial=false must modify the buffer";
    EXPECT_EQ(combined_api, combined_manual) << "initial=false must combine via fmix32Combined(h(row), prior)";
}


// ──────────────────────────────────────────────────────────────────────
// 4. Distinct values produce mostly distinct hashes (birthday sanity).
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnVectorDistinctHashes)
{
    const size_t n = 4096;
    // Sequential integers: a trivial hash would produce identical hashes.
    auto col = ColumnUInt32::create();
    for (size_t i = 0; i < n; ++i)
        col->insert(static_cast<UInt64>(i));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);

    // Count unique hashes.
    std::vector<uint32_t> sorted = out;
    std::sort(sorted.begin(), sorted.end());
    const size_t unique = static_cast<size_t>(std::unique(sorted.begin(), sorted.end()) - sorted.begin());

    // For 4096 distinct inputs and a good 32-bit hash the expected number of
    // collisions is ~4096^2 / 2^33 ≈ 2.  Allow up to 1% collision rate.
    EXPECT_GT(unique, n * 99 / 100) << "Too many hash collisions for sequential UInt32 inputs";
}


// ──────────────────────────────────────────────────────────────────────
// 5. ColumnNullable: null rows hash differently from their non-null twins.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, NullableNullStateDiscrimination)
{
    // Build a Nullable(UInt32) column with alternating null / non-null rows,
    // all with the same underlying value (42).
    const size_t n = 64;
    auto nested = ColumnUInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < n; ++i)
    {
        nested->insert(static_cast<UInt64>(42));
        null_map->insert(static_cast<UInt64>(i % 2 == 0 ? 0 : 1)); // even=not-null, odd=null
    }
    auto col = ColumnNullable::create(std::move(nested), std::move(null_map));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);

    // Even-indexed rows (not-null) and odd-indexed rows (null) should differ.
    for (size_t i = 0; i + 1 < n; i += 2)
        EXPECT_NE(out[i], out[i + 1]) << "Null and non-null rows with identical nested bytes must hash differently (row " << i << ")";
}


// ──────────────────────────────────────────────────────────────────────
// 5b. ColumnNullable: NULL rows hash independently of their hidden nested
//     payload, so SQL-equal NULL keys always route to the same shard.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, NullableNullPayloadIndependence)
{
    // Every row is NULL, but the (hidden) nested payload differs per row.
    const size_t n = 64;
    auto nested = ColumnUInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < n; ++i)
    {
        nested->insert(static_cast<UInt64>(i * 2654435761ULL)); // distinct hidden payloads
        null_map->insert(static_cast<UInt64>(1)); // all rows NULL
    }
    auto col = ColumnNullable::create(std::move(nested), std::move(null_map));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);

    for (size_t i = 1; i < n; ++i)
        EXPECT_EQ(out[i], out[0]) << "Two NULL rows with different hidden payloads must hash identically (row " << i << ")";
}


// ──────────────────────────────────────────────────────────────────────
// 5c. ColumnNullable payload independence under composition (initial=false),
//     which exercises the transient-scratch path of computeHashInto.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, NullableNullPayloadIndependenceComposed)
{
    // First key column identical across rows; second key column all-NULL with
    // differing hidden payloads. The composed key hash must be identical per row.
    const size_t n = 64;
    auto col0 = ColumnUInt32::create();
    auto nested = ColumnUInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < n; ++i)
    {
        col0->insert(static_cast<UInt64>(123)); // identical first key
        nested->insert(static_cast<UInt64>(i * 2654435761ULL)); // distinct hidden payloads
        null_map->insert(static_cast<UInt64>(1)); // all rows NULL
    }
    auto nullable = ColumnNullable::create(std::move(nested), std::move(null_map));

    std::vector<uint32_t> out(n);
    col0->computeHashInto(0, n, out.data(), true); // initial
    nullable->computeHashInto(0, n, out.data(), false); // non-initial: scratch path

    for (size_t i = 1; i < n; ++i)
        EXPECT_EQ(out[i], out[0]) << "Composed key (k, NULL) must be payload-independent (row " << i << ")";
}


// ──────────────────────────────────────────────────────────────────────
// 6. ColumnString: zero-length, 1-byte, and longer strings all differ.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnStringTailHandling)
{
    std::vector<std::string> vals;
    // Lengths 0 through 20 bytes.
    for (size_t len = 0; len <= 20; ++len)
        vals.push_back(std::string(len, 'a'));
    const size_t n = vals.size();
    auto col = makeStringColumn(vals);

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);

    // All strings differ, so all hashes should differ.
    std::vector<uint32_t> sorted = out;
    std::sort(sorted.begin(), sorted.end());
    const size_t unique = static_cast<size_t>(std::unique(sorted.begin(), sorted.end()) - sorted.begin());
    EXPECT_EQ(unique, n) << "Strings of distinct lengths should all hash differently";
}


// ──────────────────────────────────────────────────────────────────────
// 7. ColumnFixedString: length participates in the hash.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnFixedStringLengthParticipates)
{
    // Two ColumnFixedString columns with the same bytes but different widths
    // (padded with zeros) should produce different hashes.
    const size_t n = 8;
    const size_t width4 = 4;
    const size_t width8 = 8;

    auto col4 = ColumnFixedString::create(width4);
    auto col8 = ColumnFixedString::create(width8);
    for (size_t i = 0; i < n; ++i)
    {
        // Same first 4 bytes in both columns.
        char buf4[4] = {'A', 'B', 'C', 'D'};
        char buf8[8] = {'A', 'B', 'C', 'D', 0, 0, 0, 0};
        col4->insertData(buf4, sizeof(buf4));
        col8->insertData(buf8, sizeof(buf8));
    }

    std::vector<uint32_t> h4(n);
    std::vector<uint32_t> h8(n);
    col4->computeHashInto(0, n, h4.data(), true);
    col8->computeHashInto(0, n, h8.data(), true);

    EXPECT_NE(h4, h8) << "ColumnFixedString with the same byte content but different widths must hash differently";
}


// ──────────────────────────────────────────────────────────────────────
// 8. Distributional uniformity for UInt32 K=1, P=64.
//    Chi-squared test: counts per partition should be roughly equal.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, DistributionUniformityUInt32K1P64)
{
    const size_t total_rows = 1 << 16; // 65536
    const size_t num_parts = 64;

    auto col = ColumnUInt32::create();
    for (size_t i = 0; i < total_rows; ++i)
        col->insert(static_cast<UInt64>(rng()));

    std::vector<uint32_t> hashes(total_rows);
    col->computeHashInto(0, total_rows, hashes.data(), true);

    std::vector<size_t> counts(num_parts, 0);
    for (auto h : hashes)
        counts[(static_cast<uint64_t>(h) * num_parts) >> 32]++;

    // Expected count per partition.
    const double expected = static_cast<double>(total_rows) / static_cast<double>(num_parts);

    double chi2 = 0.0;
    for (size_t p = 0; p < num_parts; ++p)
    {
        const double delta = static_cast<double>(counts[p]) - expected;
        chi2 += (delta * delta) / expected;
    }

    // Critical value for chi2 with df=63, p=0.001 is ~103.
    // A good hash should score well below this.
    EXPECT_LT(chi2, 103.0) << "Hash distribution is non-uniform (chi2=" << chi2 << " for P=64)";
}


// ──────────────────────────────────────────────────────────────────────
// 9. Multi-column (K=4) uniformity, P=64.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, DistributionUniformityUInt32K4P64)
{
    const size_t total_rows = 1 << 16;
    const size_t num_key_cols = 4;
    const size_t num_parts = 64;

    std::vector<ColumnUInt32::MutablePtr> cols;
    for (size_t k = 0; k < num_key_cols; ++k)
    {
        auto col = ColumnUInt32::create();
        for (size_t i = 0; i < total_rows; ++i)
            col->insert(static_cast<UInt64>(rng()));
        cols.push_back(std::move(col));
    }

    std::vector<uint32_t> hashes(total_rows);
    for (size_t k = 0; k < num_key_cols; ++k)
        cols[k]->computeHashInto(0, total_rows, hashes.data(), k == 0);

    std::vector<size_t> counts(num_parts, 0);
    for (auto h : hashes)
        counts[(static_cast<uint64_t>(h) * num_parts) >> 32]++;

    const double expected = static_cast<double>(total_rows) / static_cast<double>(num_parts);
    double chi2 = 0.0;
    for (size_t p = 0; p < num_parts; ++p)
    {
        const double delta = static_cast<double>(counts[p]) - expected;
        chi2 += (delta * delta) / expected;
    }
    EXPECT_LT(chi2, 103.0) << "K=4 hash distribution non-uniform (chi2=" << chi2 << " for P=64)";
}


// ──────────────────────────────────────────────────────────────────────
// 10. ColumnDecimal: Decimal32 / Decimal64 basic sanity.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnDecimalDistinctHashes)
{
    const size_t n = 256;

    auto col32 = ColumnDecimal<Decimal32>::create(0, 4);
    auto col64 = ColumnDecimal<Decimal64>::create(0, 4);
    for (size_t i = 0; i < n; ++i)
    {
        col32->insert(DecimalField<Decimal32>(Decimal32(static_cast<Int32>(i)), 4));
        col64->insert(DecimalField<Decimal64>(Decimal64(static_cast<Int64>(i)), 4));
    }

    std::vector<uint32_t> out32(n);
    std::vector<uint32_t> out64(n);
    col32->computeHashInto(0, n, out32.data(), true);
    col64->computeHashInto(0, n, out64.data(), true);

    auto count_unique = [](std::vector<uint32_t> v)
    {
        std::sort(v.begin(), v.end());
        return static_cast<size_t>(std::unique(v.begin(), v.end()) - v.begin());
    };

    EXPECT_GT(count_unique(out32), n * 99 / 100) << "Decimal32: too many hash collisions";
    EXPECT_GT(count_unique(out64), n * 99 / 100) << "Decimal64: too many hash collisions";
}


// ──────────────────────────────────────────────────────────────────────
// 10b. ColumnBFloat16: hash the raw 16-bit value, not a Float32-truncated int.
//      Distinct fractional values must not collapse, and -0.0 must hash like +0.0.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnBFloat16RawBitsAndSignedZero)
{
    // Distinct fractional values: the buggy `static_cast<uint32_t>(v)` path truncated all of
    // these to 0 (or invoked UB for negatives), collapsing every key onto one shard.
    auto col = ColumnBFloat16::create();
    const std::vector<float> vals = {0.1f, 0.2f, 0.3f, 1.5f, -1.5f, 100.25f, 12345.0f};
    for (float f : vals)
        col->insertValue(BFloat16(f));
    const size_t n = vals.size();

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);

    std::vector<uint32_t> sorted = out;
    std::sort(sorted.begin(), sorted.end());
    const size_t unique = static_cast<size_t>(std::unique(sorted.begin(), sorted.end()) - sorted.begin());
    EXPECT_EQ(unique, n) << "Distinct BFloat16 values must hash distinctly (raw-bit hashing)";

    // Signed zero: -0.0 and +0.0 compare equal, so they must hash identically.
    auto zeros = ColumnBFloat16::create();
    zeros->insertValue(BFloat16(0.0f));
    zeros->insertValue(BFloat16(-0.0f));
    std::vector<uint32_t> hz(2);
    zeros->computeHashInto(0, 2, hz.data(), true);
    EXPECT_EQ(hz[0], hz[1]) << "BFloat16 -0.0 and +0.0 must hash identically";
}


// ──────────────────────────────────────────────────────────────────────
// 11. ColumnTuple: chaining of children, range-split, and order sensitivity.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnTupleCompositionAndOrder)
{
    const size_t n = 1024;
    ColumnPtr a = makeUInt32Column(randomUInts(n));
    ColumnPtr b = makeStringColumn([&]
    {
        std::vector<std::string> v(n);
        for (auto & s : v)
            s = std::string(rng() % 17, 'x');
        return v;
    }());

    auto tuple_ab = ColumnTuple::create(Columns{a, b});
    expectRangeSplitConsistent(*tuple_ab);

    // A tuple hash must equal manually chaining its children with the same flags.
    std::vector<uint32_t> manual(n);
    a->computeHashInto(0, n, manual.data(), true);
    b->computeHashInto(0, n, manual.data(), false);

    std::vector<uint32_t> via_tuple(n);
    tuple_ab->computeHashInto(0, n, via_tuple.data(), true);
    EXPECT_EQ(manual, via_tuple);

    // Order matters: tuple(a, b) and tuple(b, a) must differ for most rows.
    auto tuple_ba = ColumnTuple::create(Columns{b, a});
    std::vector<uint32_t> ab(n);
    std::vector<uint32_t> ba(n);
    tuple_ab->computeHashInto(0, n, ab.data(), true);
    tuple_ba->computeHashInto(0, n, ba.data(), true);

    size_t differ = 0;
    for (size_t i = 0; i < n; ++i)
        differ += (ab[i] != ba[i]);
    EXPECT_GT(differ, n * 9 / 10) << "tuple(a,b) and tuple(b,a) should differ for most rows";
}


// ──────────────────────────────────────────────────────────────────────
// 12. ColumnArray: range-split consistency and length sensitivity.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnArrayRangeSplitAndLength)
{
    const size_t n = 2048;
    auto arr = makeUInt32ArrayColumn(n, 6);
    expectRangeSplitConsistent(*arr);

    // Arrays of the same repeated element but different lengths must not collide:
    // [7], [7, 7], [7, 7, 7], ...
    auto data = ColumnUInt32::create();
    auto offsets = ColumnUInt64::create();
    const size_t m = 16;
    size_t acc = 0;
    for (size_t i = 1; i <= m; ++i)
    {
        for (size_t j = 0; j < i; ++j)
            data->insert(static_cast<UInt64>(7));
        acc += i;
        offsets->insert(static_cast<UInt64>(acc));
    }
    auto repeated = ColumnArray::create(std::move(data), std::move(offsets));

    std::vector<uint32_t> out(m);
    repeated->computeHashInto(0, m, out.data(), true);

    std::vector<uint32_t> sorted = out;
    std::sort(sorted.begin(), sorted.end());
    const size_t unique = static_cast<size_t>(std::unique(sorted.begin(), sorted.end()) - sorted.begin());
    EXPECT_EQ(unique, m) << "Arrays of the same element but different lengths must hash differently";
}


// ──────────────────────────────────────────────────────────────────────
// 12b. ColumnArray: all-zero / repeated-zero arrays of different lengths must
//      not collapse to the same hash (and therefore all route to shard 0).
//      fmix32(0) == 0 and fmix32Combined(0, 0) == 0, so the array length must be
//      mixed explicitly; the [7]-based test above does not exercise this.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnArrayAllZeroKeysDistinct)
{
    // [], [0], [0, 0], [0, 0, 0], [0, 0, 0, 0]
    auto data = ColumnUInt32::create();
    auto offsets = ColumnUInt64::create();
    const size_t m = 5;
    size_t acc = 0;
    for (size_t len = 0; len < m; ++len)
    {
        for (size_t j = 0; j < len; ++j)
            data->insert(static_cast<UInt64>(0));
        acc += len;
        offsets->insert(static_cast<UInt64>(acc));
    }
    auto arr = ColumnArray::create(std::move(data), std::move(offsets));

    std::vector<uint32_t> out(m);
    arr->computeHashInto(0, m, out.data(), true);

    std::vector<uint32_t> sorted = out;
    std::sort(sorted.begin(), sorted.end());
    const size_t unique = static_cast<size_t>(std::unique(sorted.begin(), sorted.end()) - sorted.begin());
    EXPECT_EQ(unique, m) << "All-zero arrays of different lengths must hash differently (lengths 0..4)";
}


// ──────────────────────────────────────────────────────────────────────
// 13. ColumnMap delegates to its nested array, so it stays range-split safe.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnMapRangeSplit)
{
    const size_t n = 1024;

    // Map is stored as Array(Tuple(key, value)).
    auto keys = ColumnUInt32::create();
    auto values = ColumnUInt32::create();
    auto offsets = ColumnUInt64::create();
    size_t acc = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const size_t len = rng() % 4;
        for (size_t j = 0; j < len; ++j)
        {
            keys->insert(static_cast<UInt64>(rng()));
            values->insert(static_cast<UInt64>(rng()));
        }
        acc += len;
        offsets->insert(static_cast<UInt64>(acc));
    }
    auto kv = ColumnTuple::create(Columns{std::move(keys), std::move(values)});
    auto nested = ColumnArray::create(std::move(kv), std::move(offsets));
    auto map = ColumnMap::create(std::move(nested));

    expectRangeSplitConsistent(*map);
}


// ──────────────────────────────────────────────────────────────────────
// 14. ColumnConst broadcasts a single element's hash to every row.
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnConstBroadcast)
{
    const size_t n = 257;
    auto inner = makeUInt32Column({0x12345678U});
    auto col = ColumnConst::create(std::move(inner), n);

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);

    for (size_t i = 1; i < n; ++i)
        EXPECT_EQ(out[i], out[0]) << "All rows of a const column must hash identically (row " << i << ")";

    expectRangeSplitConsistent(*col);
}


// ──────────────────────────────────────────────────────────────────────
// 15. ColumnNullable range-split consistency (null mask mixed per row).
// ──────────────────────────────────────────────────────────────────────
TEST(ComputeHashInto, ColumnNullableRangeSplit)
{
    const size_t n = 1024;
    auto nested = ColumnUInt32::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < n; ++i)
    {
        nested->insert(static_cast<UInt64>(rng()));
        null_map->insert(static_cast<UInt64>(rng() % 5 == 0 ? 1 : 0));
    }
    auto col = ColumnNullable::create(std::move(nested), std::move(null_map));
    expectRangeSplitConsistent(*col);
}


// ──────────────────────────────────────────────────────────────────────
// 16. Representation independence (the Blocker-3 invariant).
//
//     For a composite key (prefix, c), the per-row hash must not depend on
//     how column `c` is physically stored.  A materialized column and a
//     transparent wrapper (ColumnConst / ColumnSparse / ColumnLowCardinality /
//     ColumnReplicated) of the same logical values must compose identically
//     after a shared prefix column, otherwise equal keys from different chunks
//     route to different aggregation shards / grace_hash partitions.
//
//     Before the fix the leaf columns combined the *raw* value on initial=false
//     while wrappers combined the *finalized* fmix32(raw); these tests fail in
//     that state and pass once both combine the finalized per-row hash.
// ──────────────────────────────────────────────────────────────────────
namespace
{
/// Compose `prefix` (initial=true) then `second` (initial=false); return the per-row hash.
std::vector<uint32_t> composeKey(const IColumn & prefix, const IColumn & second)
{
    const size_t n = prefix.size();
    std::vector<uint32_t> h(n);
    prefix.computeHashInto(0, n, h.data(), true);
    second.computeHashInto(0, n, h.data(), false);
    return h;
}

/// Wrap a single-row column into a ColumnConst broadcasting that value to `n` rows.
ColumnPtr makeConst(ColumnPtr single_value, size_t n)
{
    return ColumnConst::create(std::move(single_value), n);
}
}

TEST(ComputeHashInto, ReprIndependenceConstUInt64)
{
    const size_t n = 777;
    const UInt64 value = 0x0123456789ABCDEFULL;
    auto prefix = makeUInt32Column(randomUInts(n));

    auto materialized = ColumnUInt64::create();
    for (size_t i = 0; i < n; ++i)
        materialized->insert(value);

    auto one = ColumnUInt64::create();
    one->insert(value);
    auto as_const = makeConst(std::move(one), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized UInt64 and ColumnConst(UInt64) of the same value must compose identically";
}

TEST(ComputeHashInto, ReprIndependenceConstString)
{
    const size_t n = 513;
    const std::string value = "representation-independent";
    auto prefix = makeUInt32Column(randomUInts(n));

    auto materialized = makeStringColumn(std::vector<std::string>(n, value));

    auto as_const = makeConst(makeStringColumn({value}), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized String and ColumnConst(String) of the same value must compose identically";
}

TEST(ComputeHashInto, ReprIndependenceConstTuple)
{
    // A composite second key must also compose representation-independently: streaming
    // tuple elements into the prior hash would differ from a ColumnConst(Tuple), which
    // combines the finalized tuple hash.
    const size_t n = 401;
    auto prefix = makeUInt32Column(randomUInts(n));

    auto build_tuple = [](size_t rows) -> MutableColumnPtr
    {
        auto t_u32 = ColumnUInt32::create();
        auto t_str = ColumnString::create();
        for (size_t i = 0; i < rows; ++i)
        {
            t_u32->insert(static_cast<UInt64>(0xABCDEFu));
            t_str->insertData("tup", 3);
        }
        MutableColumns elems;
        elems.push_back(std::move(t_u32));
        elems.push_back(std::move(t_str));
        return ColumnTuple::create(std::move(elems));
    };

    auto materialized = build_tuple(n);
    auto as_const = makeConst(build_tuple(1), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized Tuple and ColumnConst(Tuple) of the same value must compose identically";
}

TEST(ComputeHashInto, ReprIndependenceConstArray)
{
    // ColumnArray must combine the finalized per-row hash on the non-initial path, so a
    // materialized Array and a ColumnConst(Array) of the same value compose identically.
    const size_t n = 333;
    auto prefix = makeUInt32Column(randomUInts(n));

    auto build_array = [](size_t rows) -> MutableColumnPtr
    {
        auto data = ColumnUInt32::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        UInt64 off = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            data->insert(static_cast<UInt64>(10));
            data->insert(static_cast<UInt64>(20));
            data->insert(static_cast<UInt64>(30));
            off += 3;
            offsets->insert(off);
        }
        return ColumnArray::create(std::move(data), std::move(offsets));
    };

    auto materialized = build_array(n);
    auto as_const = makeConst(build_array(1), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized Array and ColumnConst(Array) of the same value must compose identically";
}

TEST(ComputeHashInto, ReprIndependenceConstDecimal64)
{
    const size_t n = 401;
    auto prefix = makeUInt32Column(randomUInts(n));

    auto materialized = ColumnDecimal<Decimal64>::create(0, 4);
    for (size_t i = 0; i < n; ++i)
        materialized->insert(DecimalField<Decimal64>(Decimal64(static_cast<Int64>(1234567)), 4));

    auto one = ColumnDecimal<Decimal64>::create(0, 4);
    one->insert(DecimalField<Decimal64>(Decimal64(static_cast<Int64>(1234567)), 4));
    auto as_const = makeConst(std::move(one), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized Decimal64 and ColumnConst(Decimal64) of the same value must compose identically";
}

TEST(ComputeHashInto, ReprIndependenceConstFixedString)
{
    const size_t n = 333;
    const size_t width = 8;
    const char bytes[width] = {'f', 'i', 'x', 'e', 'd', '!', 0, 0};
    auto prefix = makeUInt32Column(randomUInts(n));

    auto materialized = ColumnFixedString::create(width);
    for (size_t i = 0; i < n; ++i)
        materialized->insertData(bytes, width);

    auto one = ColumnFixedString::create(width);
    one->insertData(bytes, width);
    auto as_const = makeConst(std::move(one), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized FixedString and ColumnConst(FixedString) of the same value must compose identically";
}

// ColumnSparse exercises the per-row gather combine path (the same fmix32Combined(value, prior)
// shape that ColumnLowCardinality and ColumnReplicated use via ColumnIndex), with values that
// vary per row rather than a single broadcast constant.
TEST(ComputeHashInto, ReprIndependenceSparseUInt64)
{
    const size_t n = 1000;
    auto prefix = makeUInt32Column(randomUInts(n));

    // Default value 0 everywhere except a few ascending non-default positions.
    const std::vector<size_t> positions = {3, 50, 51, 200, 777, 999};
    const std::vector<UInt64> values = {7, 7, 8, 123456789ULL, 42, 0xFFFFFFFFFFFFFFFFULL};

    auto materialized = ColumnUInt64::create();
    for (size_t i = 0; i < n; ++i)
    {
        UInt64 v = 0;
        for (size_t k = 0; k < positions.size(); ++k)
            if (positions[k] == i)
                v = values[k];
        materialized->insert(v);
    }

    auto sp_values = ColumnUInt64::create();
    sp_values->insert(static_cast<UInt64>(0)); // index 0 is the default
    for (auto v : values)
        sp_values->insert(v);
    auto sp_offsets = ColumnUInt64::create();
    for (auto p : positions)
        sp_offsets->insert(static_cast<UInt64>(p));
    auto sparse = ColumnSparse::create(std::move(sp_values), std::move(sp_offsets), n);

    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *sparse))
        << "Materialized UInt64 and ColumnSparse(UInt64) with equal logical values must compose identically";
}


// ──────────────────────────────────────────────────────────────────────
// 17. Wide (128/256-bit) numeric types exercise the >8-byte folding path in
//     hashValueRaw32 (ColumnVector) and hashDecimalRaw32 (ColumnDecimal),
//     which the smaller types never reach. A bug in that fold would silently
//     change routing for wide keys while passing the rest of the suite, so
//     each type is checked for distinct hashes, row-range split independence,
//     and initial=false (Const) composition.
// ──────────────────────────────────────────────────────────────────────
namespace
{
size_t countUnique(std::vector<uint32_t> v)
{
    std::sort(v.begin(), v.end());
    return static_cast<size_t>(std::unique(v.begin(), v.end()) - v.begin());
}

/// Populate all 16 bytes of a UInt128 from `i` so the 4-byte folding loop matters
/// (a fold that ignored the high word would collapse many of these to one hash).
UInt128 makeWide128(size_t i)
{
    UInt128 v = 0;
    v |= UInt128(0x1111111111111111ULL + i);
    v |= UInt128(0x9E3779B97F4A7C15ULL ^ i) << 64;
    return v;
}

/// Populate all 32 bytes of a UInt256 from `i`.
UInt256 makeWide256(size_t i)
{
    UInt256 v = 0;
    v |= UInt256(0x1111111111111111ULL + i);
    v |= UInt256(0x2222222222222222ULL ^ i) << 64;
    v |= UInt256(0x3333333333333333ULL + i) << 128;
    v |= UInt256(0x9E3779B97F4A7C15ULL ^ i) << 192;
    return v;
}
}

TEST(ComputeHashInto, ColumnVectorUInt128WideFold)
{
    const size_t n = 1024;
    auto col = ColumnUInt128::create();
    for (size_t i = 0; i < n; ++i)
        col->insertValue(makeWide128(i));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);
    EXPECT_GT(countUnique(out), n * 99 / 100) << "UInt128: too many hash collisions (folding bug?)";

    expectRangeSplitConsistent(*col);

    auto prefix = makeUInt32Column(randomUInts(n));
    auto materialized = ColumnUInt128::create();
    for (size_t i = 0; i < n; ++i)
        materialized->insertValue(makeWide128(42));
    auto one = ColumnUInt128::create();
    one->insertValue(makeWide128(42));
    auto as_const = makeConst(std::move(one), n);
    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized UInt128 and ColumnConst(UInt128) of the same value must compose identically";
}

TEST(ComputeHashInto, ColumnVectorUInt256WideFold)
{
    const size_t n = 1024;
    auto col = ColumnUInt256::create();
    for (size_t i = 0; i < n; ++i)
        col->insertValue(makeWide256(i));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);
    EXPECT_GT(countUnique(out), n * 99 / 100) << "UInt256: too many hash collisions (folding bug?)";

    expectRangeSplitConsistent(*col);

    auto prefix = makeUInt32Column(randomUInts(n));
    auto materialized = ColumnUInt256::create();
    for (size_t i = 0; i < n; ++i)
        materialized->insertValue(makeWide256(42));
    auto one = ColumnUInt256::create();
    one->insertValue(makeWide256(42));
    auto as_const = makeConst(std::move(one), n);
    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized UInt256 and ColumnConst(UInt256) of the same value must compose identically";
}

TEST(ComputeHashInto, ColumnDecimal128WideFold)
{
    const size_t n = 1024;
    auto col = ColumnDecimal<Decimal128>::create(0, 4);
    for (size_t i = 0; i < n; ++i)
        col->insert(DecimalField<Decimal128>(Decimal128(static_cast<Int128>(makeWide128(i))), 4));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);
    EXPECT_GT(countUnique(out), n * 99 / 100) << "Decimal128: too many hash collisions (folding bug?)";

    expectRangeSplitConsistent(*col);

    auto prefix = makeUInt32Column(randomUInts(n));
    const auto repr = Decimal128(static_cast<Int128>(makeWide128(42)));
    auto materialized = ColumnDecimal<Decimal128>::create(0, 4);
    for (size_t i = 0; i < n; ++i)
        materialized->insert(DecimalField<Decimal128>(repr, 4));
    auto one = ColumnDecimal<Decimal128>::create(0, 4);
    one->insert(DecimalField<Decimal128>(repr, 4));
    auto as_const = makeConst(std::move(one), n);
    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized Decimal128 and ColumnConst(Decimal128) of the same value must compose identically";
}

TEST(ComputeHashInto, ColumnDecimal256WideFold)
{
    const size_t n = 1024;
    auto col = ColumnDecimal<Decimal256>::create(0, 4);
    for (size_t i = 0; i < n; ++i)
        col->insert(DecimalField<Decimal256>(Decimal256(static_cast<Int256>(makeWide256(i))), 4));

    std::vector<uint32_t> out(n);
    col->computeHashInto(0, n, out.data(), true);
    EXPECT_GT(countUnique(out), n * 99 / 100) << "Decimal256: too many hash collisions (folding bug?)";

    expectRangeSplitConsistent(*col);

    auto prefix = makeUInt32Column(randomUInts(n));
    const auto repr = Decimal256(static_cast<Int256>(makeWide256(42)));
    auto materialized = ColumnDecimal<Decimal256>::create(0, 4);
    for (size_t i = 0; i < n; ++i)
        materialized->insert(DecimalField<Decimal256>(repr, 4));
    auto one = ColumnDecimal<Decimal256>::create(0, 4);
    one->insert(DecimalField<Decimal256>(repr, 4));
    auto as_const = makeConst(std::move(one), n);
    EXPECT_EQ(composeKey(*prefix, *materialized), composeKey(*prefix, *as_const))
        << "Materialized Decimal256 and ColumnConst(Decimal256) of the same value must compose identically";
}
