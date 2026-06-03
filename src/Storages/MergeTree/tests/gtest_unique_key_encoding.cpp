#include <gtest/gtest.h>

#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// Test ergonomics: encode a single row via `encodeBlock` and return its
/// encoded form. Slices each column to row `row` first so `max_size` and
/// unsupported-type checks apply only to that row, matching strict 1-row
/// semantics. Underlying production API is `encodeBlock`.
String testEncodeRow(const Columns & cols, size_t row, size_t max_size = 4096)
{
    Columns row_cols;
    row_cols.reserve(cols.size());
    for (const auto & c : cols)
        row_cols.push_back(c->cut(row, 1));
    std::vector<String> out;
    UniqueKeyEncoding::encodeBlock(row_cols, /*permutation=*/nullptr, max_size, out);
    return out.at(0);
}

/// Helper: pack two scalars of the same column type into a 2-row column for
/// pairwise-order testing.
template <typename Col, typename T>
auto makePair(T a, T b)
{
    auto col = Col::create();
    col->insert(Field(a));
    col->insert(Field(b));
    return col;
}

/// Deep comparator: lexicographic column-level comparison across all key
/// columns, matching what `IColumn::compareAt` would do.
int columnCompareRows(const Columns & cols, size_t ra, size_t rb)
{
    for (const auto & col : cols)
    {
        int r = col->compareAt(ra, rb, *col, 1);
        if (r != 0)
            return r;
    }
    return 0;
}

/// Agreement check: signum(memcmp(encoded(a), encoded(b))) == signum(compareAt(a, b)).
/// Returns true on agreement.
bool agrees(const Columns & cols, size_t ra, size_t rb, size_t max_size = 4096)
{
    String ea = testEncodeRow(cols, ra, max_size);
    String eb = testEncodeRow(cols, rb, max_size);

    int mem = std::memcmp(ea.data(), eb.data(),
                          std::min(ea.size(), eb.size()));
    int cmp;
    if (mem != 0)
        cmp = mem > 0 ? 1 : -1;
    else if (ea.size() != eb.size())
        cmp = ea.size() > eb.size() ? 1 : -1;
    else
        cmp = 0;

    int expected = columnCompareRows(cols, ra, rb);
    int expected_sign = (expected > 0) - (expected < 0);
    int actual_sign = (cmp > 0) - (cmp < 0);
    return expected_sign == actual_sign;
}

}

/// ---------------------------------------------------------------------------
/// Unsigned integer ordering — random pair fuzz + boundary values.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, UInt64Ordering)
{
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(0xDEADBEEF);

    auto col = ColumnUInt64::create();
    /// Boundary values first.
    for (UInt64 v : {UInt64(0), UInt64(1), std::numeric_limits<UInt64>::max() - 1, std::numeric_limits<UInt64>::max()})
        col->insert(Field(v));
    /// Random values.
    for (size_t i = 0; i < 100; ++i)
        col->insert(Field(rng()));

    Columns cols{std::move(col)};
    const size_t n = cols[0]->size();

    for (size_t i = 0; i < 1000; ++i)
    {
        size_t a = rng() % n;
        size_t b = rng() % n;
        EXPECT_TRUE(agrees(cols, a, b)) << "UInt64 order disagreement at rows " << a << "," << b;
    }
}

TEST(UniqueKeyEncoding, AllUnsignedWidths)
{
    auto col8 = makePair<ColumnUInt8>(UInt8(0), UInt8(255));
    Columns cols{std::move(col8)};
    EXPECT_TRUE(agrees(cols, 0, 1));

    auto col16 = makePair<ColumnUInt16>(UInt16(0), UInt16(65535));
    cols = {std::move(col16)};
    EXPECT_TRUE(agrees(cols, 0, 1));

    auto col32 = makePair<ColumnUInt32>(UInt32(0), UInt32(4294967295U));
    cols = {std::move(col32)};
    EXPECT_TRUE(agrees(cols, 0, 1));
}

TEST(UniqueKeyEncoding, SignedIntOrdering)
{
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(0xCAFEBABE);

    auto col = ColumnInt64::create();
    for (Int64 v : {std::numeric_limits<Int64>::min(), Int64(-1), Int64(0), Int64(1), std::numeric_limits<Int64>::max()})
        col->insert(Field(v));
    for (size_t i = 0; i < 100; ++i)
        col->insert(Field(static_cast<Int64>(rng())));

    Columns cols{std::move(col)};
    const size_t n = cols[0]->size();

    for (size_t i = 0; i < 1000; ++i)
    {
        size_t a = rng() % n;
        size_t b = rng() % n;
        EXPECT_TRUE(agrees(cols, a, b)) << "Int64 order disagreement at rows " << a << "," << b
                                          << " vals=" << typeid_cast<const ColumnInt64 &>(*cols[0]).getData()[a]
                                          << "," << typeid_cast<const ColumnInt64 &>(*cols[0]).getData()[b];
    }
}

TEST(UniqueKeyEncoding, SignedIntNarrowWidths)
{
    auto col8 = makePair<ColumnInt8>(Int8(-128), Int8(127));
    Columns cols{std::move(col8)};
    EXPECT_TRUE(agrees(cols, 0, 1));

    auto col16 = makePair<ColumnInt16>(Int16(-32768), Int16(32767));
    cols = {std::move(col16)};
    EXPECT_TRUE(agrees(cols, 0, 1));

    auto col32 = makePair<ColumnInt32>(std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max());
    cols = {std::move(col32)};
    EXPECT_TRUE(agrees(cols, 0, 1));
}

/// ---------------------------------------------------------------------------
/// Floats — IEEE-754 total-order including -0 / +0 and ±Inf.
/// NaN ordering is documented as "NaN sorts after +Inf"; we verify the
/// relative ordering among finite values but do not test NaN-vs-NaN.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, FloatOrdering)
{
    auto col = ColumnFloat64::create();
    std::vector<Float64> vals = {
        -std::numeric_limits<Float64>::infinity(),
        std::numeric_limits<Float64>::lowest(),
        Float64(-1.5),
        Float64(-0.0),
        Float64(0.0),
        Float64(1.5),
        std::numeric_limits<Float64>::max(),
        std::numeric_limits<Float64>::infinity(),
    };
    for (auto v : vals)
        col->insert(Field(v));

    Columns cols{std::move(col)};

    /// After encoding, memcmp order must match the order we listed vals in.
    std::vector<String> encoded(vals.size());
    for (size_t i = 0; i < vals.size(); ++i)
        encoded[i] = testEncodeRow(cols, i, 4096);

    for (size_t i = 1; i < vals.size(); ++i)
    {
        /// -0.0 and +0.0 compare equal in IEEE-754 and are now canonicalized
        /// at encode time to the same bytes — see the dedicated SignedZero*
        /// tests below. Skip the adjacent-pair strict-less assertion for this
        /// specific pair; all other adjacent pairs must strictly increase.
        if (vals[i - 1] == Float64(-0.0) && vals[i] == Float64(0.0))
        {
            EXPECT_EQ(encoded[i - 1], encoded[i])
                << "-0.0 and +0.0 must encode to identical bytes after canonicalization";
            continue;
        }
        EXPECT_LT(std::memcmp(encoded[i - 1].data(), encoded[i].data(),
                              std::min(encoded[i - 1].size(), encoded[i].size())),
                  0) << "Float encoding order broken between " << vals[i-1] << " and " << vals[i];
    }
}

/// ---------------------------------------------------------------------------
/// Signed-zero canonicalization — -0.0 and +0.0 must encode to identical bytes
/// across both Float32 and Float64 columns. Required for UNIQUE KEY dedup
/// correctness under the memcmp-based probe path (otherwise the two rows
/// would coexist as phantom duplicates).
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, SignedZeroFloat64Canonicalized)
{
    auto col = ColumnFloat64::create();
    col->insert(Field(Float64(-0.0)));
    col->insert(Field(Float64(0.0)));
    Columns cols{std::move(col)};

    String e_neg = testEncodeRow(cols, 0, 4096);
    String e_pos = testEncodeRow(cols, 1, 4096);
    EXPECT_EQ(e_neg, e_pos)
        << "Float64 -0.0 and +0.0 must encode to identical bytes (UNIQUE KEY dedup requirement)";
}

TEST(UniqueKeyEncoding, SignedZeroFloat32Canonicalized)
{
    auto col = ColumnFloat32::create();
    col->insert(Field(Float32(-0.0f)));
    col->insert(Field(Float32(0.0f)));
    Columns cols{std::move(col)};

    String e_neg = testEncodeRow(cols, 0, 4096);
    String e_pos = testEncodeRow(cols, 1, 4096);
    EXPECT_EQ(e_neg, e_pos)
        << "Float32 -0.0 and +0.0 must encode to identical bytes (UNIQUE KEY dedup requirement)";
}

/// ---------------------------------------------------------------------------
/// NaN canonicalization — every NaN bit pattern (any sign, any payload) must
/// encode to the same sentinel bytes, and those bytes must sort AFTER any
/// finite float and AFTER +Inf, matching `compareAt(nan_direction_hint=1)`
/// semantics.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, SignedNaNFloat64Canonicalized)
{
    /// Build a column with a variety of NaN bit patterns: positive quiet NaN,
    /// negative quiet NaN, signaling NaN with non-zero payload, plus the
    /// anchors +Inf / -Inf and a couple of finite values.
    auto col = ColumnFloat64::create();

    /// Row 0: -Inf (anchor — sorts before all finite).
    col->insert(Field(-std::numeric_limits<Float64>::infinity()));
    /// Row 1: a finite value.
    col->insert(Field(Float64(-1.5)));
    /// Row 2: +0.0 (anchor — middle).
    col->insert(Field(Float64(0.0)));
    /// Row 3: a finite value.
    col->insert(Field(Float64(1.5)));
    /// Row 4: +Inf (anchor — largest non-NaN).
    col->insert(Field(std::numeric_limits<Float64>::infinity()));

    /// NaN rows — 4 distinct bit patterns.
    /// Row 5: positive quiet NaN (std::numeric_limits default).
    col->insert(Field(std::numeric_limits<Float64>::quiet_NaN()));
    /// Row 6: negative-sign NaN (same payload, sign bit flipped).
    {
        UInt64 bits = std::bit_cast<UInt64>(std::numeric_limits<Float64>::quiet_NaN());
        bits |= 0x8000000000000000ULL;
        col->insert(Field(std::bit_cast<Float64>(bits)));
    }
    /// Row 7: positive NaN with a non-default mantissa payload.
    {
        UInt64 bits = 0x7FF8ABCDEF012345ULL; /// exponent all 1s + non-zero mantissa.
        col->insert(Field(std::bit_cast<Float64>(bits)));
    }
    /// Row 8: negative-sign NaN with a signaling-range payload (msb-of-mantissa=0, rest non-zero).
    {
        UInt64 bits = 0xFFF0000000000001ULL; /// sign=1, exp all 1s, mantissa = 1 (signaling).
        col->insert(Field(std::bit_cast<Float64>(bits)));
    }

    Columns cols{std::move(col)};

    std::vector<String> enc(cols[0]->size());
    for (size_t i = 0; i < cols[0]->size(); ++i)
        enc[i] = testEncodeRow(cols, i, 4096);

    /// All four NaN rows must encode to identical bytes.
    ASSERT_EQ(enc[5], enc[6]) << "positive and negative quiet NaN must canonicalize to same bytes";
    ASSERT_EQ(enc[5], enc[7]) << "different NaN payloads must canonicalize to same bytes";
    ASSERT_EQ(enc[5], enc[8]) << "signaling NaN must canonicalize to same bytes";

    /// NaN encoding must sort strictly after +Inf (row 4).
    EXPECT_LT(std::memcmp(enc[4].data(), enc[5].data(), std::min(enc[4].size(), enc[5].size())), 0)
        << "NaN must sort after +Inf";
    /// NaN must sort strictly after every finite value and -Inf.
    for (size_t finite_row : {0u, 1u, 2u, 3u})
    {
        EXPECT_LT(std::memcmp(enc[finite_row].data(), enc[5].data(),
                              std::min(enc[finite_row].size(), enc[5].size())), 0)
            << "NaN must sort after non-NaN row " << finite_row;
    }
}

TEST(UniqueKeyEncoding, SignedNaNFloat32Canonicalized)
{
    auto col = ColumnFloat32::create();

    /// Row 0: -Inf.
    col->insert(Field(-std::numeric_limits<Float32>::infinity()));
    /// Row 1: +Inf.
    col->insert(Field(std::numeric_limits<Float32>::infinity()));
    /// Row 2: finite.
    col->insert(Field(Float32(100.0f)));

    /// Row 3: positive quiet NaN.
    col->insert(Field(std::numeric_limits<Float32>::quiet_NaN()));
    /// Row 4: negative-sign NaN.
    {
        UInt32 bits = std::bit_cast<UInt32>(std::numeric_limits<Float32>::quiet_NaN());
        bits |= 0x80000000U;
        col->insert(Field(std::bit_cast<Float32>(bits)));
    }
    /// Row 5: different payload.
    {
        UInt32 bits = 0x7FC12345U; /// exponent all 1s, mantissa non-zero.
        col->insert(Field(std::bit_cast<Float32>(bits)));
    }
    /// Row 6: negative-sign NaN with distinct payload.
    {
        UInt32 bits = 0xFF800001U; /// sign=1, exp all 1s, mantissa=1.
        col->insert(Field(std::bit_cast<Float32>(bits)));
    }

    Columns cols{std::move(col)};

    std::vector<String> enc(cols[0]->size());
    for (size_t i = 0; i < cols[0]->size(); ++i)
        enc[i] = testEncodeRow(cols, i, 4096);

    ASSERT_EQ(enc[3], enc[4]) << "Float32 positive vs negative NaN must canonicalize";
    ASSERT_EQ(enc[3], enc[5]) << "Float32 different NaN payloads must canonicalize";
    ASSERT_EQ(enc[3], enc[6]) << "Float32 signaling NaN must canonicalize";

    /// NaN sorts after +Inf.
    EXPECT_LT(std::memcmp(enc[1].data(), enc[3].data(), std::min(enc[1].size(), enc[3].size())), 0)
        << "Float32 NaN must sort after +Inf";
    /// NaN sorts after -Inf and any finite.
    for (size_t finite_row : {0u, 2u})
    {
        EXPECT_LT(std::memcmp(enc[finite_row].data(), enc[3].data(),
                              std::min(enc[finite_row].size(), enc[3].size())), 0)
            << "Float32 NaN must sort after row " << finite_row;
    }
}

TEST(UniqueKeyEncoding, Float32Ordering)
{
    auto col = ColumnFloat32::create();
    for (Float32 v : {Float32(-100.0f), Float32(-0.001f), Float32(0.0f), Float32(0.001f), Float32(100.0f)})
        col->insert(Field(v));

    Columns cols{std::move(col)};
    for (size_t i = 0; i + 1 < cols[0]->size(); ++i)
        EXPECT_TRUE(agrees(cols, i, i + 1));
}

/// ---------------------------------------------------------------------------
/// String — escaped-null, embedded-null, empty, prefix relationships.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, StringOrdering)
{
    auto col = ColumnString::create();
    col->insert(Field(String("")));            // 0
    col->insert(Field(String("\x00", 1)));     // 1 embedded null
    col->insert(Field(String("\x00\x00", 2))); // 2 two embedded nulls
    col->insert(Field(String("a")));           // 3
    col->insert(Field(String("ab")));          // 4 (prefix relationship)
    col->insert(Field(String("abc")));         // 5
    col->insert(Field(String("b")));           // 6
    col->insert(Field(String("z")));           // 7

    Columns cols{std::move(col)};
    const size_t n = cols[0]->size();
    for (size_t a = 0; a < n; ++a)
        for (size_t b = 0; b < n; ++b)
            EXPECT_TRUE(agrees(cols, a, b)) << "string pair a=" << a << " b=" << b;
}

TEST(UniqueKeyEncoding, StringPrefixFree)
{
    /// The key correctness property: for two strings a, b where a is a prefix
    /// of b, memcmp(encode(a), encode(b)) < 0, never > 0. Tests that the
    /// terminator '\0\x00' sorts below any continuation of escaped content.
    auto col = ColumnString::create();
    col->insert(Field(String("hello")));
    col->insert(Field(String("hello world")));
    col->insert(Field(String("hello\x00world", 11)));

    Columns cols{std::move(col)};
    EXPECT_TRUE(agrees(cols, 0, 1));
    EXPECT_TRUE(agrees(cols, 1, 2));
    EXPECT_TRUE(agrees(cols, 0, 2));
}

/// ---------------------------------------------------------------------------
/// String encoder byte-equivalence — the encoder is independently re-derived
/// from the spec ("'\0' -> '\0\x01'; terminator '\0\x00'") and compared to
/// `appendEscapedString` via `encodeBlock` for a battery of widths and embedded
/// '\0' positions. Catches any drift in the bulk-memcpy / memchr-segment fast
/// path away from the per-byte semantics.
/// ---------------------------------------------------------------------------
namespace
{

/// Reference encoder mirroring the spec one byte at a time. Output is the
/// raw escaped value (no per-row null flag, no compound-key concat) — same
/// payload `appendEscapedString` writes after the optional Nullable byte.
String referenceEscape(const char * data, size_t size)
{
    String out;
    out.reserve(size + 2);
    for (size_t i = 0; i < size; ++i)
    {
        out.push_back(data[i]);
        if (data[i] == '\0')
            out.push_back('\x01');
    }
    out.push_back('\0');
    out.push_back('\0');
    return out;
}

void expectEncodesAs(const String & input, const String & expected_payload)
{
    auto col = ColumnString::create();
    col->insert(Field(input));
    Columns cols{std::move(col)};
    String got = testEncodeRow(cols, 0, /*max_size=*/4096);
    EXPECT_EQ(got, expected_payload)
        << "input size=" << input.size()
        << " encoded size=" << got.size()
        << " expected size=" << expected_payload.size();
}

/// Block-level variant — exercises `encodeBlock` → `appendStringColumn`
/// → `appendEscapedString`.
void expectBlockEncodesAs(const std::vector<String> & inputs)
{
    auto col = ColumnString::create();
    for (const auto & s : inputs)
        col->insert(Field(s));
    Columns cols{std::move(col)};
    std::vector<String> encoded;
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, /*max_size=*/4096, encoded);
    ASSERT_EQ(encoded.size(), inputs.size());
    for (size_t r = 0; r < inputs.size(); ++r)
    {
        const String expected = referenceEscape(inputs[r].data(), inputs[r].size());
        EXPECT_EQ(encoded[r], expected)
            << "row=" << r
            << " input size=" << inputs[r].size()
            << " encoded size=" << encoded[r].size()
            << " expected size=" << expected.size();
    }
}

}

TEST(UniqueKeyEncoding, StringEncoderByteEquivalent)
{
    /// No-NUL widths spanning cache-line boundaries — fast path.
    {
        // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
        std::mt19937_64 rng(0xA5A5A5A5);
        for (size_t width : {size_t(0), size_t(1), size_t(7), size_t(8), size_t(15),
                              size_t(16), size_t(17), size_t(31), size_t(32),
                              size_t(33), size_t(63), size_t(64), size_t(65),
                              size_t(127), size_t(128), size_t(255), size_t(256), size_t(1023)})
        {
            String s(width, 0);
            for (char & c : s)
                c = 'a' + (rng() % 26);
            expectEncodesAs(s, referenceEscape(s.data(), s.size()));
        }
    }

    /// Embedded-NUL position variants — slow path. Empty input must still
    /// emit the 2-byte "\0\0" terminator.
    expectEncodesAs(String(), String("\0\0", 2));
    for (const String & s : {
        String("\x00""abc", 4),                          /// '\0' at start
        String("abc\x00", 4),                            /// '\0' at end
        String("abc\x00""def", 7),                       /// '\0' in middle
        String(1, '\0'), String(4, '\0'), String(16, '\0'), /// all-'\0'
        String("\x00""a\x00""b\x00", 5),                 /// alternating w/ trailing '\0'
        String("\x00\x00""ab", 4),                       /// adjacent '\0' at start
        String("ab\x00\x00", 4),                         /// adjacent '\0' at end
    })
        expectEncodesAs(s, referenceEscape(s.data(), s.size()));

    /// Fuzz — 500 random widths, ~25% '\0' density.
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(0x1234567890ABCDEFULL);
    for (size_t trial = 0; trial < 500; ++trial)
    {
        String s(rng() % 257, 0);
        for (char & c : s)
        {
            UInt32 r = rng() & 0xFF;
            c = (r & 0x3) == 0 ? '\0' : static_cast<char>(1 + (r % 255));
        }
        expectEncodesAs(s, referenceEscape(s.data(), s.size()));
    }
}

TEST(UniqueKeyEncoding, StringBlockEncoderByteEquivalent)
{
    /// Block-level path: `encodeBlock` → `appendStringColumn`. Mix of edge
    /// cases and varying widths in a single block.
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(0xB10C);
    std::vector<String> rows = {
        String(),
        String("hello", 5),
        String("\x00", 1),
        String("\x00""abc", 4),
        String("abc\x00", 4),
        String("a\x00""b", 3),
        String(4, '\0'),
        String("\x00""x\x00""y\x00", 5),
        String("\x00\x00""ab", 4),
        String("ab\x00\x00", 4),
    };
    for (size_t width : {size_t(0), size_t(1), size_t(8), size_t(16), size_t(64), size_t(256), size_t(1023)})
    {
        String s(width, 0);
        for (char & c : s)
            c = 'a' + (rng() % 26);
        rows.push_back(std::move(s));
    }
    expectBlockEncodesAs(rows);
}

TEST(UniqueKeyEncoding, FixedStringBlockEncoderByteEquivalentEmbeddedNull)
{
    /// FixedString rows can legally contain '\0' bytes (zero-padded short
    /// values). `appendFixedStringColumn` appends the raw N bytes per row
    /// (no '\0' escaping — the fixed width is the framing).
    constexpr size_t N = 8;
    auto col = ColumnFixedString::create(N);
    /// Row 0: all-printable
    col->insertData("hello!!!", N);
    /// Row 1: all-'\0' (zero-padded empty)
    col->insertData("\x00\x00\x00\x00\x00\x00\x00\x00", N);
    /// Row 2: mixed '\0' bytes embedded
    col->insertData("ab\x00""cd\x00""ef", N);
    /// Row 3: '\0' at boundary
    col->insertData("\x00""1234567", N);
    Columns cols{std::move(col)};

    std::vector<String> got;
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, /*max_size=*/4096, got);
    ASSERT_EQ(got.size(), 4u);

    /// Reference: row-encode a 1-col FixedString block — raw N bytes.
    auto expected_for = [&](const std::array<char, N> & data) {
        auto rcol = ColumnFixedString::create(N);
        rcol->insertData(data.data(), N);
        Columns rcols{std::move(rcol)};
        return testEncodeRow(rcols, 0, 4096);
    };
    constexpr std::array<char, N> row0 = {'h', 'e', 'l', 'l', 'o', '!', '!', '!'};
    constexpr std::array<char, N> row1 = {0, 0, 0, 0, 0, 0, 0, 0};
    constexpr std::array<char, N> row2 = {'a', 'b', 0, 'c', 'd', 0, 'e', 'f'};
    constexpr std::array<char, N> row3 = {0, '1', '2', '3', '4', '5', '6', '7'};
    EXPECT_EQ(got[0], expected_for(row0));
    EXPECT_EQ(got[1], expected_for(row1));
    EXPECT_EQ(got[2], expected_for(row2));
    EXPECT_EQ(got[3], expected_for(row3));
}

TEST(UniqueKeyEncoding, FixedStringEncoderByteEquivalent)
{
    /// FixedString routes through `appendFixedStringColumn`, which appends raw
    /// bytes (no escape — width-prefixed). Verify direct memcpy for widths
    /// 8 / 16 / 24 / 32 / 64.
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(0xFEEDFACE);
    for (size_t width : {size_t(8), size_t(16), size_t(24), size_t(32), size_t(64)})
    {
        auto col = ColumnFixedString::create(width);
        std::vector<String> inputs;
        for (int i = 0; i < 4; ++i)
        {
            String s(width, 0);
            for (char & c : s)
                c = static_cast<char>(rng() & 0xFF); /// includes '\0' occasionally
            col->insertData(s.data(), width);
            inputs.push_back(std::move(s));
        }
        Columns cols{std::move(col)};

        for (size_t r = 0; r < inputs.size(); ++r)
        {
            String got = testEncodeRow(cols, r, 4096);
            EXPECT_EQ(got.size(), width)
                << "FixedString(" << width << ") row " << r;
            EXPECT_EQ(0, std::memcmp(got.data(), inputs[r].data(), width))
                << "FixedString(" << width << ") row " << r;
        }
    }
}

/// ---------------------------------------------------------------------------
/// FixedString — raw bytes.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, FixedStringOrdering)
{
    auto col = ColumnFixedString::create(4);
    col->insertData("aaaa", 4);
    col->insertData("aaab", 4);
    col->insertData("zzzz", 4);

    Columns cols{std::move(col)};
    EXPECT_TRUE(agrees(cols, 0, 1));
    EXPECT_TRUE(agrees(cols, 1, 2));
}

/// ---------------------------------------------------------------------------
/// UUID — 128-bit, big-endian.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, UUIDOrdering)
{
    auto col = ColumnUUID::create();
    col->insert(Field(UUID(UInt128(0))));
    col->insert(Field(UUID(UInt128(1))));
    /// Set hi-limb to test MSB-first.
    UInt128 big = 0;
    big.items[UInt128::_impl::big(0)] = 1ULL;
    col->insert(Field(UUID(big)));

    Columns cols{std::move(col)};
    EXPECT_TRUE(agrees(cols, 0, 1));
    EXPECT_TRUE(agrees(cols, 1, 2));
    EXPECT_TRUE(agrees(cols, 0, 2));
}

/// ---------------------------------------------------------------------------
/// Enum8 / Enum16 — Enum columns are stored as `ColumnVector<Int8/Int16>`
/// (no dedicated `ColumnEnum`), so `IColumn::getDataType()` reports `Int8`
/// / `Int16`. Pin signed-int ordering across the negative→positive seam
/// for both widths, with a Nullable wrapper at the end to exercise the
/// null-map path (Enum types frequently appear as `Nullable(Enum8)`).
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, EnumOrdering)
{
    auto i8 = ColumnInt8::create();
    for (Int8 v : {Int8(-128), Int8(-1), Int8(0), Int8(1), Int8(127)})
        i8->insertValue(v);
    Columns cols_i8{std::move(i8)};
    for (size_t i = 0; i + 1 < cols_i8[0]->size(); ++i)
        EXPECT_TRUE(agrees(cols_i8, i, i + 1)) << "Enum8/Int8 per-row row " << i;

    auto i16 = ColumnInt16::create();
    for (Int16 v : {Int16(-32768), Int16(-1), Int16(0), Int16(1), Int16(32767)})
        i16->insertValue(v);
    Columns cols_i16{std::move(i16)};
    for (size_t i = 0; i + 1 < cols_i16[0]->size(); ++i)
        EXPECT_TRUE(agrees(cols_i16, i, i + 1)) << "Enum16/Int16 per-row row " << i;

}

/// ---------------------------------------------------------------------------
/// Decimal32/64/128/256 — sign-flip over the native integer.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, Decimal64Ordering)
{
    auto col = ColumnDecimal<Decimal64>::create(0, /*scale=*/4);
    for (Int64 raw : {Int64(-1000000), Int64(-1), Int64(0), Int64(1), Int64(1000000)})
        col->insert(DecimalField<Decimal64>(Decimal64(raw), 4));

    Columns cols{std::move(col)};
    for (size_t i = 0; i + 1 < cols[0]->size(); ++i)
        EXPECT_TRUE(agrees(cols, i, i + 1));
}

/// ---------------------------------------------------------------------------
/// Date / Date32 / DateTime / DateTime64.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, DateVariants)
{
    auto date_col = ColumnUInt16::create();         // Date (UInt16)
    date_col->insert(Field(UInt16(0)));             // 1970-01-01
    date_col->insert(Field(UInt16(65535)));         // late 2149
    Columns cols{std::move(date_col)};
    EXPECT_TRUE(agrees(cols, 0, 1));

    auto date32_col = ColumnInt32::create();        // Date32 (Int32)
    date32_col->insert(Field(Int32(-1)));
    date32_col->insert(Field(Int32(0)));
    date32_col->insert(Field(Int32(1)));
    cols = {std::move(date32_col)};
    for (size_t i = 0; i + 1 < cols[0]->size(); ++i)
        EXPECT_TRUE(agrees(cols, i, i + 1));

    auto dt_col = ColumnUInt32::create();           // DateTime (UInt32)
    dt_col->insert(Field(UInt32(0)));
    dt_col->insert(Field(UInt32(2147483647U)));
    cols = {std::move(dt_col)};
    EXPECT_TRUE(agrees(cols, 0, 1));

    auto dt64_col = ColumnDecimal<DateTime64>::create(0, 3);
    dt64_col->insert(DecimalField<DateTime64>(DateTime64(-1000), 3));
    dt64_col->insert(DecimalField<DateTime64>(DateTime64(0), 3));
    dt64_col->insert(DecimalField<DateTime64>(DateTime64(1000), 3));
    cols = {std::move(dt64_col)};
    for (size_t i = 0; i + 1 < cols[0]->size(); ++i)
        EXPECT_TRUE(agrees(cols, i, i + 1));
}

/// ---------------------------------------------------------------------------
/// Nullable — NULL sorts AFTER non-NULL (matches `compareAt` with
/// `nulls_direction=1`, the direction the writer's `stableGetPermutation`
/// uses). Between non-NULLs the inner-column order applies.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, NullableOrdering)
{
    auto nested = ColumnUInt64::create();
    nested->insert(Field(UInt64(0)));            // 0: non-null 0
    nested->insert(Field(UInt64(0)));            // 1: null (placeholder for nullmap)
    nested->insert(Field(UInt64(100)));          // 2: non-null 100
    auto null_map = ColumnUInt8::create();
    null_map->insert(Field(UInt64(0)));
    null_map->insert(Field(UInt64(1)));
    null_map->insert(Field(UInt64(0)));

    auto col = ColumnNullable::create(std::move(nested), std::move(null_map));
    Columns cols{std::move(col)};

    /// Row 1 is null — should sort strictly after rows 0 and 2.
    String e0 = testEncodeRow(cols, 0, 4096);
    String e1 = testEncodeRow(cols, 1, 4096);
    String e2 = testEncodeRow(cols, 2, 4096);

    EXPECT_GT(std::memcmp(e1.data(), e0.data(), std::min(e0.size(), e1.size())), 0);
    EXPECT_GT(std::memcmp(e1.data(), e2.data(), std::min(e1.size(), e2.size())), 0);
    EXPECT_LT(std::memcmp(e0.data(), e2.data(), std::min(e0.size(), e2.size())), 0);

    /// Cross-check via the encoder's documented contract:
    /// `memcmp(encode(A), encode(B))` agrees with `compareAt(A, B, 1)`.
    EXPECT_TRUE(agrees(cols, 0, 1));
    EXPECT_TRUE(agrees(cols, 1, 2));
    EXPECT_TRUE(agrees(cols, 0, 2));
}

/// ---------------------------------------------------------------------------
/// Compound keys — concatenation preserves order when each component is
/// prefix-free and individually order-preserving.
/// Shuffle-and-sort: encode a bunch of random rows, sort by encoding, and
/// assert the sort order matches a column-level multi-key sort.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, CompoundKeyShuffleSort)
{
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(0x12345678);

    auto col_u = ColumnUInt32::create();
    auto col_s = ColumnString::create();
    auto col_i = ColumnInt64::create();

    const size_t N = 200;
    for (size_t i = 0; i < N; ++i)
    {
        col_u->insert(Field(UInt32(rng() % 10)));
        String s(rng() % 12, 'a');
        for (char & c : s)
            c = 'a' + (rng() % 26);
        col_s->insert(Field(s));
        col_i->insert(Field(static_cast<Int64>(rng() % 1000) - 500));
    }

    Columns cols{std::move(col_u), std::move(col_s), std::move(col_i)};

    std::vector<size_t> rows(N);
    std::iota(rows.begin(), rows.end(), 0);

    auto order_by_encoding = rows;
    std::sort(order_by_encoding.begin(), order_by_encoding.end(),
              [&](size_t a, size_t b)
              {
                  String ea = testEncodeRow(cols, a, 4096);
                  String eb = testEncodeRow(cols, b, 4096);
                  int m = std::memcmp(ea.data(), eb.data(), std::min(ea.size(), eb.size()));
                  if (m != 0)
                      return m < 0;
                  return ea.size() < eb.size();
              });

    auto order_by_compare = rows;
    std::sort(order_by_compare.begin(), order_by_compare.end(),
              [&](size_t a, size_t b) { return columnCompareRows(cols, a, b) < 0; });

    /// The orderings should be identical when no duplicates, stable when equal.
    /// We check adjacent-pair agreement for robustness to tie-breaking.
    for (size_t i = 0; i + 1 < N; ++i)
    {
        size_t a = order_by_encoding[i];
        size_t b = order_by_encoding[i + 1];
        EXPECT_LE(columnCompareRows(cols, a, b), 0) << "adjacent encoding-sort pair out of compare-order at i=" << i;
    }
}

/// ---------------------------------------------------------------------------
/// Size limit — top-level rejection.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, SizeLimitRejection)
{
    auto col = ColumnString::create();
    col->insert(Field(String(300, 'x')));

    Columns cols{std::move(col)};

    /// Too small — should throw BAD_ARGUMENTS.
    EXPECT_THROW(testEncodeRow(cols, 0, 100), DB::Exception);

    /// Sufficient — should succeed.
    EXPECT_NO_THROW(testEncodeRow(cols, 0, 1024));
}

/// ---------------------------------------------------------------------------
/// encodeBlock — column-wise batch encoder. Tests below pin format
/// invariants; size-limit and empty-input contracts.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, EncodeBlockBasic)
{
    auto col = ColumnUInt64::create();
    col->insert(Field(UInt64(1)));
    col->insert(Field(UInt64(2)));
    col->insert(Field(UInt64(3)));

    Columns cols{std::move(col)};
    std::vector<String> encoded;
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 4096, encoded);

    ASSERT_EQ(encoded.size(), 3u);
    EXPECT_LT(std::memcmp(encoded[0].data(), encoded[1].data(), encoded[0].size()), 0);
    EXPECT_LT(std::memcmp(encoded[1].data(), encoded[2].data(), encoded[1].size()), 0);
}

/// Size-limit enforcement: an oversized row must throw BAD_ARGUMENTS.
TEST(UniqueKeyEncoding, EncodeBlockSizeLimitRejection)
{
    auto col = ColumnString::create();
    col->insert(Field(String(300, 'x'))); /// 300-char string = ~302 encoded bytes
    col->insert(Field(String(10, 'y')));

    Columns cols{std::move(col)};

    std::vector<String> out;
    EXPECT_THROW(UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 100, out), DB::Exception);

    /// Sufficient limit — both rows should encode successfully.
    EXPECT_NO_THROW(UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 1024, out));
    ASSERT_EQ(out.size(), 2u);
}

/// Well-formed permutation must succeed and produce one encoded row per input.
TEST(UniqueKeyEncoding, EncodeBlockPermutationValidAccepted)
{
    auto col = ColumnUInt64::create();
    col->insert(Field(UInt64{10}));
    col->insert(Field(UInt64{20}));
    col->insert(Field(UInt64{30}));
    Columns cols{std::move(col)};

    IColumn::Permutation perm{2, 0, 1};
    std::vector<String> out;
    EXPECT_NO_THROW(UniqueKeyEncoding::encodeBlock(cols, &perm, 256, out));
    ASSERT_EQ(out.size(), 3u);
}

/// Empty-block: must produce an empty output vector without touching max_size.
TEST(UniqueKeyEncoding, EncodeBlockEmptyBlock)
{
    auto col = ColumnUInt64::create();
    Columns cols{std::move(col)};

    std::vector<String> out;
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 256, out);
    EXPECT_TRUE(out.empty());
}

/// ---------------------------------------------------------------------------
/// Microbenchmarks — informational only; no gate.
/// ---------------------------------------------------------------------------
TEST(UniqueKeyEncoding, DISABLED_MicrobenchUInt641M)
{
    constexpr size_t N = 1'000'000;
    auto col = ColumnUInt64::create();
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(42);
    for (size_t i = 0; i < N; ++i)
        col->insert(Field(rng()));
    Columns cols{std::move(col)};

    std::vector<String> out;
    auto t0 = std::chrono::steady_clock::now();
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 256, out);
    auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    std::cerr << "[microbench] UInt64 1M: " << (dt / N) << " ns/key total " << dt / 1'000'000 << " ms\n";
    EXPECT_EQ(out.size(), N);
}

TEST(UniqueKeyEncoding, DISABLED_MicrobenchString321M)
{
    constexpr size_t N = 1'000'000;
    auto col = ColumnString::create();
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(43);
    for (size_t i = 0; i < N; ++i)
    {
        String s(32, 0);
        for (char & c : s)
            c = 'a' + (rng() % 26);
        col->insert(Field(s));
    }
    Columns cols{std::move(col)};

    std::vector<String> out;
    auto t0 = std::chrono::steady_clock::now();
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 256, out);
    auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    std::cerr << "[microbench] String32 1M: " << (dt / N) << " ns/key total " << dt / 1'000'000 << " ms\n";
    EXPECT_EQ(out.size(), N);
}

TEST(UniqueKeyEncoding, DISABLED_MicrobenchCompoundUInt64String1M)
{
    constexpr size_t N = 1'000'000;
    auto col_u = ColumnUInt64::create();
    auto col_s = ColumnString::create();
    // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    std::mt19937_64 rng(44);
    for (size_t i = 0; i < N; ++i)
    {
        col_u->insert(Field(rng()));
        String s(16, 0);
        for (char & c : s)
            c = 'a' + (rng() % 26);
        col_s->insert(Field(s));
    }
    Columns cols{std::move(col_u), std::move(col_s)};

    std::vector<String> out;
    auto t0 = std::chrono::steady_clock::now();
    UniqueKeyEncoding::encodeBlock(cols, /*permutation=*/nullptr, 256, out);
    auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - t0).count();
    std::cerr << "[microbench] (UInt64,String16) 1M: " << (dt / N) << " ns/key total " << dt / 1'000'000 << " ms\n";
    EXPECT_EQ(out.size(), N);
}

/// ---------- unsupported-type rejection ----------
/// `UniqueKeyEncoding` does not yet implement Array / Tuple /
/// LowCardinality. The DDL surface accepts bare column identifiers
/// without filtering by data type, so the encoder is the contract that
/// rejects these — `encodeBlock` throws `NOT_IMPLEMENTED`. Pin the
/// specific error code here so a future encoder addition is forced to
/// update this test.

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace
{
    void expectEncodeRowThrowsNotImplemented(const Columns & cols, const char * type_label)
    {
        try
        {
            (void)testEncodeRow(cols, /*row=*/0, /*max_size=*/256);
            FAIL() << "encodeBlock on " << type_label << " did not throw";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED)
                << type_label << " should throw NOT_IMPLEMENTED, got " << e.code() << ": " << e.message();
        }
    }
}

TEST(UniqueKeyEncoding, ArrayThrowsNotImplemented)
{
    auto type_arr = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    auto col = type_arr->createColumn();
    col->insertDefault();

    Columns cols;
    cols.push_back(std::move(col));
    expectEncodeRowThrowsNotImplemented(cols, "Array(UInt64)");
}

TEST(UniqueKeyEncoding, TupleThrowsNotImplemented)
{
    DataTypes inner{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()};
    auto type_tup = std::make_shared<DataTypeTuple>(inner);
    auto col = type_tup->createColumn();
    col->insertDefault();

    Columns cols;
    cols.push_back(std::move(col));
    expectEncodeRowThrowsNotImplemented(cols, "Tuple(UInt64,UInt64)");
}

TEST(UniqueKeyEncoding, LowCardinalityThrowsNotImplemented)
{
    auto type_lc = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto col = type_lc->createColumn();
    col->insertDefault();

    Columns cols;
    cols.push_back(std::move(col));
    expectEncodeRowThrowsNotImplemented(cols, "LowCardinality(String)");
}
