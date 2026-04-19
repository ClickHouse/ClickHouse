/// Unit tests for the COLUMNAR_V1 wire format encoder/decoder.
///
/// There are two distinct wire formats for Array columns:
///
/// CH→WASM (encoder, input direction):
///   desc.offsets_offset → outer uint32[N+1]  (row boundaries)
///   desc.data_offset    → for Array(String): inner_offsets[M+1] + null-terminated chars
///                         for Array(fixed):  packed elements (M * elem_size)
///
/// WASM→CH (decoder, output direction):
///   desc.data_offset → outer uint32[N+1] followed immediately by nested data
///                      (everything is sequential, no separate offsets_offset)
///
/// Tests:
///   1. ColumnString encoder/decoder round-trip (COL_BYTES — formats are the same both ways)
///   2. Array(String) encoder: verify correct wire bytes written by buildColDescriptor+writeColData
///   3. Array(UInt64) encoder: verify correct wire bytes
///   4. COL_COMPLEX decoder: Array(Tuple(UInt64, Float64)) with manually-crafted WASM-output bytes
///   5. COL_IS_REPEAT string: encode a periodic string column, check wire layout
///   6. COL_IS_REPEAT fixed: encode a periodic UInt64 column, check wire layout
///   7. COL_IS_REPEAT no-repeat: column with no period stays as plain COL_BYTES

#include <gtest/gtest.h>
#include <cstring>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/UserDefined/ColumnarV1Wire.h>

using namespace DB;
using namespace DB::ColumnarV1;

namespace
{

// Build a complete single-column COLUMNAR_V1 wire buffer (CH→WASM format).
std::vector<uint8_t> encodeCHColumn(const IColumn * col, uint32_t num_rows)
{
    ColDescriptor desc{};
    uint32_t cursor = COLUMNAR_HEADER_BYTES + COLUMNAR_DESC_BYTES;
    cursor = buildColDescriptor(col, /*is_const=*/false, /*is_nullable=*/false, num_rows, cursor, desc);

    std::vector<uint8_t> buf(cursor, 0);

    uint32_t one = 1;
    std::memcpy(buf.data(),     &num_rows, 4);
    std::memcpy(buf.data() + 4, &one,      4);
    std::memcpy(buf.data() + COLUMNAR_HEADER_BYTES, &desc, COLUMNAR_DESC_BYTES);

    writeColData(col, /*is_nullable=*/false, num_rows, desc, {buf.data(), buf.size()});
    return buf;
}

ColDescriptor readDesc(const std::vector<uint8_t> & buf)
{
    ColDescriptor desc{};
    std::memcpy(&desc, buf.data() + COLUMNAR_HEADER_BYTES, COLUMNAR_DESC_BYTES);
    return desc;
}

} // anonymous namespace

// ── ColumnString round-trip (COL_BYTES format is the same in both directions) ─

TEST(ColumnarV1Wire, StringEncodeDecodeRoundTrip)
{
    auto col = ColumnString::create();
    col->insertData("hello", 5);
    col->insertData("world", 5);
    col->insertData("", 0);

    auto buf = encodeCHColumn(col.get(), 3);

    ColDescriptor desc = readDesc(buf);
    EXPECT_EQ(desc.type, COL_BYTES);

    auto result_type = std::make_shared<DataTypeString>();
    auto decoded = readColumnarOutput({buf.data(), buf.size()}, result_type, 3);

    const auto * decoded_str = typeid_cast<const ColumnString *>(decoded.get());
    ASSERT_NE(decoded_str, nullptr);
    ASSERT_EQ(decoded_str->size(), 3u);
    EXPECT_EQ(decoded_str->getDataAt(0), "hello");
    EXPECT_EQ(decoded_str->getDataAt(1), "world");
    EXPECT_EQ(decoded_str->getDataAt(2), "");
}

// ── Array(String) encoder: verify CH→WASM wire bytes ─────────────────────────
//
// Input: 2 rows — row 0 = ["foo", "bar"], row 1 = ["baz"]
// Expected layout:
//   offsets_offset → uint32[3] = {0, 2, 3}   (outer row→element boundaries)
//   data_offset →   uint32[4] = {0, 4, 8, 12} (inner per-string offsets w/ null terminators)
//                   bytes     = "foo\0bar\0baz\0"

TEST(ColumnarV1Wire, ArrayStringEncoderLayout)
{
    auto nested_str = ColumnString::create();
    nested_str->insertData("foo", 3);
    nested_str->insertData("bar", 3);
    nested_str->insertData("baz", 3);

    auto offsets = ColumnUInt64::create();
    offsets->getData().push_back(2);   // row 0 ends at element 2
    offsets->getData().push_back(3);   // row 1 ends at element 3

    auto arr_col = ColumnArray::create(std::move(nested_str), std::move(offsets));

    auto buf = encodeCHColumn(arr_col.get(), 2);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type, COL_COMPLEX);
    EXPECT_NE(desc.offsets_offset, 0u);
    EXPECT_NE(desc.data_offset, 0u);
    EXPECT_GT(desc.data_offset, desc.offsets_offset);

    // Outer offsets (at offsets_offset): {0, 2, 3}
    const uint32_t * outer = reinterpret_cast<const uint32_t *>(buf.data() + desc.offsets_offset);
    EXPECT_EQ(outer[0], 0u);
    EXPECT_EQ(outer[1], 2u);  // row 0 has 2 elements
    EXPECT_EQ(outer[2], 3u);  // row 1 has 1 element

    // Inner offsets (at data_offset): {0, 4, 8, 12} (each string + '\0')
    const uint32_t * inner = reinterpret_cast<const uint32_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(inner[0], 0u);
    EXPECT_EQ(inner[1], 4u);   // "foo\0"
    EXPECT_EQ(inner[2], 8u);   // "bar\0"
    EXPECT_EQ(inner[3], 12u);  // "baz\0"

    // Chars (right after inner offsets)
    const uint8_t * chars = buf.data() + desc.data_offset + 4u * 4u;
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars), 3), "foo");
    EXPECT_EQ(chars[3], '\0');
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars + 4), 3), "bar");
    EXPECT_EQ(chars[7], '\0');
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars + 8), 3), "baz");
    EXPECT_EQ(chars[11], '\0');
}

// ── Array(UInt64) encoder: verify CH→WASM wire bytes ─────────────────────────
//
// Input: 3 rows — row 0 = [10, 20], row 1 = [], row 2 = [30]
// Expected:
//   offsets_offset → uint32[4] = {0, 2, 2, 3}  (outer)
//   data_offset    → uint64[3] = {10, 20, 30}  (packed)

TEST(ColumnarV1Wire, ArrayUInt64EncoderLayout)
{
    auto nested_u64 = ColumnUInt64::create();
    nested_u64->getData().push_back(10);
    nested_u64->getData().push_back(20);
    nested_u64->getData().push_back(30);

    auto offsets = ColumnUInt64::create();
    offsets->getData().push_back(2);
    offsets->getData().push_back(2);
    offsets->getData().push_back(3);

    auto arr_col = ColumnArray::create(std::move(nested_u64), std::move(offsets));

    auto buf = encodeCHColumn(arr_col.get(), 3);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type, COL_COMPLEX);

    // Outer offsets (at offsets_offset): {0, 2, 2, 3}
    const uint32_t * outer = reinterpret_cast<const uint32_t *>(buf.data() + desc.offsets_offset);
    EXPECT_EQ(outer[0], 0u);
    EXPECT_EQ(outer[1], 2u);
    EXPECT_EQ(outer[2], 2u);
    EXPECT_EQ(outer[3], 3u);

    // Elements (at data_offset): 3 × uint64
    EXPECT_EQ(desc.data_size, 3u * sizeof(uint64_t));
    const uint64_t * elems = reinterpret_cast<const uint64_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(elems[0], 10u);
    EXPECT_EQ(elems[1], 20u);
    EXPECT_EQ(elems[2], 30u);
}

// ── COL_COMPLEX decoder: Array(Tuple(UInt64, Float64)) ───────────────────────
//
// Manually construct a WASM-output buffer (sequential layout) for 2 rows:
//   row 0: [(10, 1.5), (20, 2.5)]
//   row 1: [(30, 3.5)]
//
// WASM output layout (data section, sequential from data_offset):
//   outer_offsets: uint32[3] = {0, 2, 3}     12 bytes
//   uint64[3]    = {10, 20, 30}              24 bytes  (Tuple field 0)
//   float64[3]   = {1.5, 2.5, 3.5}          24 bytes  (Tuple field 1)

TEST(ColumnarV1Wire, DecodeArrayOfTupleUInt64Float64)
{
    const uint32_t num_rows = 2;
    const uint32_t M        = 3;

    constexpr uint32_t data_off       = COLUMNAR_HEADER_BYTES + COLUMNAR_DESC_BYTES;
    constexpr uint32_t outer_bytes    = (num_rows + 1u) * 4u;  // 12
    constexpr uint32_t u64_bytes      = M * 8u;                // 24
    constexpr uint32_t f64_bytes      = M * 8u;                // 24
    constexpr uint32_t data_size      = outer_bytes + u64_bytes + f64_bytes;
    constexpr uint32_t total          = data_off + data_size;

    std::vector<uint8_t> buf(total, 0);

    // Header
    std::memcpy(buf.data(),     &num_rows, 4);
    uint32_t one = 1;
    std::memcpy(buf.data() + 4, &one, 4);

    // Descriptor — outer offsets embedded at start of data section
    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = data_size;
    std::memcpy(buf.data() + COLUMNAR_HEADER_BYTES, &desc, COLUMNAR_DESC_BYTES);

    // Outer offsets
    uint32_t * outer = reinterpret_cast<uint32_t *>(buf.data() + data_off);
    outer[0] = 0;  outer[1] = 2;  outer[2] = 3;

    // Tuple field 0: UInt64
    uint64_t * u64 = reinterpret_cast<uint64_t *>(buf.data() + data_off + outer_bytes);
    u64[0] = 10;  u64[1] = 20;  u64[2] = 30;

    // Tuple field 1: Float64
    double * f64 = reinterpret_cast<double *>(buf.data() + data_off + outer_bytes + u64_bytes);
    f64[0] = 1.5;  f64[1] = 2.5;  f64[2] = 3.5;

    // Decode
    DataTypes field_types = {std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeFloat64>()};
    auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(field_types));
    auto decoded = readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows);

    const auto * arr = typeid_cast<const ColumnArray *>(decoded.get());
    ASSERT_NE(arr, nullptr);
    EXPECT_EQ(arr->size(), 2u);

    const auto & arr_offsets = arr->getOffsets();
    EXPECT_EQ(arr_offsets[0], 2u);
    EXPECT_EQ(arr_offsets[1], 3u);

    const auto * tup = typeid_cast<const ColumnTuple *>(&arr->getData());
    ASSERT_NE(tup, nullptr);
    EXPECT_EQ(tup->size(), 3u);

    const auto & col_u64 = typeid_cast<const ColumnUInt64 &>(tup->getColumn(0));
    EXPECT_EQ(col_u64.getData()[0], 10u);
    EXPECT_EQ(col_u64.getData()[1], 20u);
    EXPECT_EQ(col_u64.getData()[2], 30u);

    const auto & col_f64 = typeid_cast<const ColumnFloat64 &>(tup->getColumn(1));
    EXPECT_DOUBLE_EQ(col_f64.getData()[0], 1.5);
    EXPECT_DOUBLE_EQ(col_f64.getData()[1], 2.5);
    EXPECT_DOUBLE_EQ(col_f64.getData()[2], 3.5);
}

// ── COL_IS_REPEAT: periodic String column ────────────────────────────────────
//
// 6 rows, pattern ["foo", "bar", "foo", "bar", "foo", "bar"] — period = 2.
// Wire should carry only 2 strings, with:
//   desc.type           = COL_BYTES | COL_IS_REPEAT
//   desc.offsets_offset = 2   (period, not a byte offset)
//   data block          = uint32[3]{0,4,8} + "foo\0bar\0"  (R+1 offsets + R strings)

TEST(ColumnarV1Wire, RepeatStringEncoding)
{
    auto col = ColumnString::create();
    for (int i = 0; i < 6; ++i)
        col->insertData(i % 2 == 0 ? "foo" : "bar", 3);

    auto buf = encodeCHColumn(col.get(), 6);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type & ~COL_IS_REPEAT, COL_BYTES);
    EXPECT_EQ(desc.type & COL_IS_REPEAT,  COL_IS_REPEAT);
    EXPECT_EQ(desc.offsets_offset, 2u);      // period = 2

    // Data block: 3 uint32 offsets + 8 bytes of strings
    const uint32_t * wire_offs = reinterpret_cast<const uint32_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(wire_offs[0], 0u);
    EXPECT_EQ(wire_offs[1], 4u);   // "foo\0"
    EXPECT_EQ(wire_offs[2], 8u);   // "bar\0"

    const uint8_t * chars = buf.data() + desc.data_offset + 3 * sizeof(uint32_t);
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars), 3), "foo");
    EXPECT_EQ(chars[3], '\0');
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars + 4), 3), "bar");
    EXPECT_EQ(chars[7], '\0');

    // Total data_size = 3*4 + 8 = 20 bytes (not 6*4 + 24 = 48 that normal would need)
    EXPECT_EQ(desc.data_size, 3u * sizeof(uint32_t) + 8u);
}

// ── COL_IS_REPEAT: periodic UInt64 column ────────────────────────────────────
//
// 9 rows, pattern [10, 20, 30, 10, 20, 30, 10, 20, 30] — period = 3.
// Wire carries only 3 uint64 values.

TEST(ColumnarV1Wire, RepeatFixed64Encoding)
{
    auto col = ColumnUInt64::create();
    for (int rep = 0; rep < 3; ++rep)
        for (uint64_t v : {10ULL, 20ULL, 30ULL})
            col->getData().push_back(v);

    auto buf = encodeCHColumn(col.get(), 9);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type & ~COL_IS_REPEAT, COL_FIXED64);
    EXPECT_EQ(desc.type & COL_IS_REPEAT,  COL_IS_REPEAT);
    EXPECT_EQ(desc.offsets_offset, 3u);          // period = 3
    EXPECT_EQ(desc.data_size, 3u * sizeof(uint64_t));  // only 3 values stored

    const uint64_t * data = reinterpret_cast<const uint64_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(data[0], 10u);
    EXPECT_EQ(data[1], 20u);
    EXPECT_EQ(data[2], 30u);
}

// ── COL_IS_REPEAT: non-periodic column stays as COL_BYTES ────────────────────
//
// 4 unique strings — no period → wire must use normal COL_BYTES encoding.

TEST(ColumnarV1Wire, RepeatStringNoRepeat)
{
    auto col = ColumnString::create();
    col->insertData("a", 1);
    col->insertData("b", 1);
    col->insertData("c", 1);
    col->insertData("d", 1);

    auto buf = encodeCHColumn(col.get(), 4);
    ColDescriptor desc = readDesc(buf);

    // Must NOT have COL_IS_REPEAT set.
    EXPECT_EQ(desc.type & COL_IS_REPEAT, 0u);
    EXPECT_EQ(desc.type, COL_BYTES);
}
