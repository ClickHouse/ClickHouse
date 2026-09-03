/// Unit tests for the ColumnBinary wire format encoder/decoder.
///
/// There are two distinct wire formats for Array columns:
///
/// CH→WASM (encoder, input direction):
///   desc.offsets_offset → outer uint32[N+1]  (row boundaries)
///   desc.data_offset    → for Array(String): inner_offsets[M+1] + chars (no null terminators)
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
///   5. Non-periodic string column encodes as plain COL_BYTES
///   6. Nullable string/fixed64 encode with COL_IS_NULLABLE and a valid null_offset

#include <gtest/gtest.h>
#include <cstring>
#include <limits>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <Formats/ColumnBinaryWire.h>

using namespace DB;
using namespace DB::ColumnBinaryWire;

namespace
{

// Build a complete single-column ColumnBinary wire buffer (CH→WASM format).
std::vector<uint8_t> encodeCHColumn(const IColumn * col, uint32_t num_rows)
{
    ColDescriptor desc{};
    uint64_t cursor = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    cursor = buildColDescriptor(col, /*is_const=*/false, /*is_nullable=*/false, num_rows, cursor, desc);

    std::vector<uint8_t> buf(cursor, 0);

    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    writeColData(col, /*is_nullable=*/false, num_rows, desc, {buf.data(), buf.size()});
    return buf;
}

ColDescriptor readDesc(const std::vector<uint8_t> & buf)
{
    ColDescriptor desc{};
    std::memcpy(&desc, buf.data() + FRAME_HEADER_BYTES, COL_DESC_BYTES);
    return desc;
}

} // anonymous namespace

// ── ColumnString round-trip (COL_BYTES format is the same in both directions) ─

TEST(ColumnBinaryWire, StringEncodeDecodeRoundTrip)
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
// Sequential layout at data_offset:
//   uint64[3] outer = {0, 2, 3}         (24 bytes)
//   uint64[4] inner = {0, 3, 6, 9}      (32 bytes, no null terminators)
//   chars = "foobarbaz"                  (9 bytes)

TEST(ColumnBinaryWire, ArrayStringEncoderLayout)
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
    EXPECT_EQ(desc.offsets_offset, 0u);  // unused for Array
    EXPECT_NE(desc.data_offset, 0u);

    // Outer offsets at data_offset: {0, 2, 3}
    const uint64_t * outer = reinterpret_cast<const uint64_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(outer[0], 0u);
    EXPECT_EQ(outer[1], 2u);  // row 0 has 2 elements
    EXPECT_EQ(outer[2], 3u);  // row 1 has 1 element

    // Inner offsets immediately after outer: {0, 3, 6, 9}
    const uint64_t * inner = outer + 3;
    EXPECT_EQ(inner[0], 0u);
    EXPECT_EQ(inner[1], 3u);  // "foo"
    EXPECT_EQ(inner[2], 6u);  // "bar"
    EXPECT_EQ(inner[3], 9u);  // "baz"

    // Chars after inner offsets
    const uint8_t * chars = reinterpret_cast<const uint8_t *>(inner + 4);
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars), 3), "foo");
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars + 3), 3), "bar");
    EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(chars + 6), 3), "baz");
}

// ── Array(UInt64) encoder: verify CH→WASM wire bytes ─────────────────────────
//
// Input: 3 rows — row 0 = [10, 20], row 1 = [], row 2 = [30]
// Sequential layout at data_offset:
//   uint64[4] outer = {0, 2, 2, 3}   (32 bytes)
//   uint64[3] elems = {10, 20, 30}   (24 bytes)

TEST(ColumnBinaryWire, ArrayUInt64EncoderLayout)
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
    EXPECT_EQ(desc.offsets_offset, 0u);  // unused for Array

    // Outer offsets at data_offset: {0, 2, 2, 3}
    const uint64_t * outer = reinterpret_cast<const uint64_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(outer[0], 0u);
    EXPECT_EQ(outer[1], 2u);
    EXPECT_EQ(outer[2], 2u);
    EXPECT_EQ(outer[3], 3u);

    // Elements immediately after outer offsets: 3 × uint64
    EXPECT_EQ(desc.data_size, 4u * sizeof(uint64_t) + 3u * sizeof(uint64_t));
    const uint64_t * elems = outer + 4;
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

TEST(ColumnBinaryWire, DecodeArrayOfTupleUInt64Float64)
{
    const uint32_t num_rows = 2;
    const uint32_t num_elems = 3;

    constexpr uint32_t data_off       = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    constexpr uint32_t outer_bytes    = (num_rows + 1u) * 8u;       // 24 (uint64 offsets)
    constexpr uint32_t u64_bytes      = num_elems * 8u;             // 24
    constexpr uint32_t f64_bytes      = num_elems * 8u;             // 24
    constexpr uint32_t data_size      = outer_bytes + u64_bytes + f64_bytes;
    constexpr uint32_t total          = data_off + data_size;

    std::vector<uint8_t> buf(total, 0);

    // Header
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    // Descriptor — outer offsets embedded at start of data section
    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = data_size;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    // Outer offsets
    uint64_t * outer = reinterpret_cast<uint64_t *>(buf.data() + data_off);
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

// ── String column encodes as COL_BYTES ───────────────────────────────────────
//
// 4 unique strings — wire must use normal COL_BYTES encoding.

TEST(ColumnBinaryWire, StringEncodesAsColBytes)
{
    auto col = ColumnString::create();
    col->insertData("a", 1);
    col->insertData("b", 1);
    col->insertData("c", 1);
    col->insertData("d", 1);

    auto buf = encodeCHColumn(col.get(), 4);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type, COL_BYTES);
}

// ── ColumnTuple encoder → COL_COMPLEX ────────────────────────────────────────
//
// 3 rows of Tuple(Float64, Float64): [(1.0,2.0), (3.0,4.0), (5.0,6.0)]
// Wire: COL_COMPLEX, no offsets (offsets_offset=0), data = field0[3] + field1[3]

TEST(ColumnBinaryWire, TupleFloat64EncoderLayout)
{
    auto f0 = ColumnFloat64::create();
    f0->getData() = {1.0, 3.0, 5.0};
    auto f1 = ColumnFloat64::create();
    f1->getData() = {2.0, 4.0, 6.0};

    Columns cols;
    cols.push_back(std::move(f0));
    cols.push_back(std::move(f1));
    auto tup = ColumnTuple::create(std::move(cols));

    auto buf = encodeCHColumn(tup.get(), 3);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type, COL_COMPLEX);
    EXPECT_EQ(desc.offsets_offset, 0u);  // no outer offsets for a top-level Tuple
    EXPECT_EQ(desc.data_size, 3u * 2u * sizeof(double));

    // Field 0 first, then field 1 (sequential in writeComplexData)
    const double * fd0 = reinterpret_cast<const double *>(buf.data() + desc.data_offset);
    EXPECT_DOUBLE_EQ(fd0[0], 1.0);
    EXPECT_DOUBLE_EQ(fd0[1], 3.0);
    EXPECT_DOUBLE_EQ(fd0[2], 5.0);

    const double * fd1 = fd0 + 3;
    EXPECT_DOUBLE_EQ(fd1[0], 2.0);
    EXPECT_DOUBLE_EQ(fd1[1], 4.0);
    EXPECT_DOUBLE_EQ(fd1[2], 6.0);
}

// ── Array(Tuple(Float64,Float64)) encoder → COL_COMPLEX ──────────────────────
//
// 2 rows: row0=[(1.0,2.0),(3.0,4.0)], row1=[(5.0,6.0)]
// Sequential layout at data_offset:
//   uint64[3] outer = {0, 2, 3}   (24 bytes)
//   float64[3] field0             (24 bytes)
//   float64[3] field1             (24 bytes)

TEST(ColumnBinaryWire, ArrayOfTupleFloat64EncoderLayout)
{
    auto f0 = ColumnFloat64::create();
    f0->getData() = {1.0, 3.0, 5.0};
    auto f1 = ColumnFloat64::create();
    f1->getData() = {2.0, 4.0, 6.0};

    Columns inner_cols;
    inner_cols.push_back(std::move(f0));
    inner_cols.push_back(std::move(f1));
    auto tup = ColumnTuple::create(std::move(inner_cols));

    auto arr_offsets = ColumnUInt64::create();
    arr_offsets->getData().push_back(2);  // row 0 ends at element 2
    arr_offsets->getData().push_back(3);  // row 1 ends at element 3

    auto arr = ColumnArray::create(std::move(tup), std::move(arr_offsets));

    auto buf = encodeCHColumn(arr.get(), 2);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type, COL_COMPLEX);
    EXPECT_EQ(desc.offsets_offset, 0u);  // unused for Array

    // Outer offsets at data_offset: {0, 2, 3}
    const uint64_t * outer = reinterpret_cast<const uint64_t *>(buf.data() + desc.data_offset);
    EXPECT_EQ(outer[0], 0u);
    EXPECT_EQ(outer[1], 2u);
    EXPECT_EQ(outer[2], 3u);

    // Nested Tuple data immediately after outer offsets: field0 then field1
    const double * fd0 = reinterpret_cast<const double *>(outer + 3);
    EXPECT_DOUBLE_EQ(fd0[0], 1.0);
    EXPECT_DOUBLE_EQ(fd0[1], 3.0);
    EXPECT_DOUBLE_EQ(fd0[2], 5.0);

    const double * fd1 = fd0 + 3;
    EXPECT_DOUBLE_EQ(fd1[0], 2.0);
    EXPECT_DOUBLE_EQ(fd1[1], 4.0);
    EXPECT_DOUBLE_EQ(fd1[2], 6.0);
}

// ── COL_VARIANT: Variant(UInt64, String) ─────────────────────────────────────
//
// 4 rows: [UInt64(10), String("hi"), UInt64(20), NULL]
// Variants in global order: variant[0]=UInt64, variant[1]=String (identity mapping).
// discriminators = {0, 1, 0, 255},  row offsets = {0, 0, 1, 0}
// Sub-columns: UInt64=[10,20] (2 rows), String=["hi"] (1 row)
//
// Wire must have:
//   null_offset    → {0,1,0,255}  discriminators
//   offsets_offset → {0,0,1,0}   row positions
//   data_offset    → K=2, records with inner ColDescriptors
//   sub-column data readable via inner descriptors

TEST(ColumnBinaryWire, VariantUInt64String)
{
    auto u64_sub = ColumnUInt64::create();
    u64_sub->getData() = {10, 20};

    auto str_sub = ColumnString::create();
    str_sub->insertData("hi", 2);

    // discriminators column (UInt8)
    auto disc_col = ColumnVector<UInt8>::create();
    disc_col->getData() = {0, 1, 0, ColumnVariant::NULL_DISCRIMINATOR};

    // offsets column (UInt64 — IColumn::Offset)
    auto offs_col = ColumnVector<UInt64>::create();
    offs_col->getData() = {0, 0, 1, 0};

    MutableColumns variants;
    variants.push_back(std::move(u64_sub));
    variants.push_back(std::move(str_sub));

    auto var_col = ColumnVariant::create(std::move(disc_col), std::move(offs_col), std::move(variants));

    uint32_t num_rows = 4;
    auto buf = encodeCHColumn(var_col.get(), num_rows);
    ColDescriptor desc = readDesc(buf);

    EXPECT_EQ(desc.type, COL_VARIANT);

    // Discriminators at null_offset
    const uint8_t * discs = buf.data() + desc.null_offset;
    EXPECT_EQ(discs[0], 0u);
    EXPECT_EQ(discs[1], 1u);
    EXPECT_EQ(discs[2], 0u);
    EXPECT_EQ(discs[3], static_cast<uint8_t>(ColumnVariant::NULL_DISCRIMINATOR));

    // Row offsets at offsets_offset (Variant uses uint32_t for per-row positions)
    const uint32_t * row_offs = reinterpret_cast<const uint32_t *>(buf.data() + desc.offsets_offset);
    EXPECT_EQ(row_offs[0], 0u);
    EXPECT_EQ(row_offs[1], 0u);
    EXPECT_EQ(row_offs[2], 1u);
    EXPECT_EQ(row_offs[3], 0u);

    // Variant header
    const uint8_t * block = buf.data() + desc.data_offset;
    uint32_t k = 0;
    std::memcpy(&k, block, 4);
    EXPECT_EQ(k, 2u);

    // Record 0: global_d=0, inner_desc for UInt64 sub-column (2 rows)
    const uint8_t * rec0 = block + 4u;
    EXPECT_EQ(rec0[0], 0u);  // global_discriminator
    ColDescriptor inner0{};
    std::memcpy(&inner0, rec0 + 4u, COL_DESC_BYTES);
    EXPECT_EQ(inner0.type, COL_FIXED64);
    EXPECT_EQ(inner0.null_offset, 2u);  // sub_rows stored for WASM navigation
    EXPECT_EQ(inner0.data_size, 2u * sizeof(uint64_t));

    const uint64_t * u64_data = reinterpret_cast<const uint64_t *>(buf.data() + inner0.data_offset);
    EXPECT_EQ(u64_data[0], 10u);
    EXPECT_EQ(u64_data[1], 20u);

    // Record 1: global_d=1, inner_desc for String sub-column (1 row)
    const uint8_t * rec1 = rec0 + 4u + COL_DESC_BYTES;
    EXPECT_EQ(rec1[0], 1u);  // global_discriminator
    ColDescriptor inner1{};
    std::memcpy(&inner1, rec1 + 4u, COL_DESC_BYTES);
    EXPECT_EQ(inner1.type, COL_BYTES);
    EXPECT_EQ(inner1.null_offset, 1u);  // sub_rows stored for WASM navigation

    const uint64_t * str_offs = reinterpret_cast<const uint64_t *>(buf.data() + inner1.offsets_offset);
    EXPECT_EQ(str_offs[0], 0u);
    EXPECT_EQ(str_offs[1], 2u);  // "hi" (no null terminator)

    const char * str_chars = reinterpret_cast<const char *>(buf.data() + inner1.data_offset);
    EXPECT_EQ(std::string_view(str_chars, 2), "hi");
}

// ── Nullable string encodes with COL_IS_NULLABLE and a valid null_offset ──────

TEST(ColumnBinaryWire, NullableStringEncoding)
{
    auto str = ColumnString::create();
    for (int i = 0; i < 6; ++i)
        str->insertData(i % 2 == 0 ? "foo" : "bar", 3);

    ColDescriptor desc{};
    buildColDescriptor(str.get(), /*is_const=*/false, /*is_nullable=*/true, 6, FRAME_HEADER_BYTES + COL_DESC_BYTES, desc);

    EXPECT_EQ(desc.type, COL_BYTES | COL_IS_NULLABLE);
    EXPECT_NE(desc.null_offset, 0u);
}

// ── Nullable UInt64 encodes with COL_IS_NULLABLE and a valid null_offset ──────

TEST(ColumnBinaryWire, NullableFixed64Encoding)
{
    auto col = ColumnUInt64::create();
    for (int rep = 0; rep < 3; ++rep)
        for (uint64_t v : {10ULL, 20ULL, 30ULL})
            col->getData().push_back(v);

    ColDescriptor desc{};
    buildColDescriptor(col.get(), /*is_const=*/false, /*is_nullable=*/true, 9, FRAME_HEADER_BYTES + COL_DESC_BYTES, desc);

    EXPECT_EQ(desc.type, COL_FIXED64 | COL_IS_NULLABLE);
    EXPECT_NE(desc.null_offset, 0u);
}

// ── COL_FIXED8 decode: result_type drives column type, not always ColumnUInt8 ─
//
// Int8 values {-1, 0, 127} encoded as COL_FIXED8; decoder must produce
// ColumnVector<Int8>, not ColumnUInt8.

TEST(ColumnBinaryWire, DecodeFixed8AsInt8)
{
    const uint32_t num_rows = 3;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(data_off + num_rows, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED8;
    desc.data_offset = data_off;
    desc.data_size   = num_rows;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    int8_t vals[3] = {-1, 0, 127};
    std::memcpy(buf.data() + data_off, vals, 3);

    auto result_type = std::make_shared<DataTypeInt8>();
    auto decoded = readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows);

    const auto * col = typeid_cast<const ColumnVector<Int8> *>(decoded.get());
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->getData()[0], int8_t(-1));
    EXPECT_EQ(col->getData()[1], int8_t(0));
    EXPECT_EQ(col->getData()[2], int8_t(127));
}

// ── Bounds check: data range overflows buffer ─────────────────────────────────
//
// COL_FIXED64 descriptor claims data_size = 3*8 = 24 bytes, but the buffer
// only has 10 bytes of payload → readColumnarOutput must throw WASM_ERROR.

TEST(ColumnBinaryWire, BoundsCheckDataOverflow)
{
    const uint32_t num_rows = 3;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(data_off + 10, 0);  // too small for 3×uint64
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED64;
    desc.data_offset = data_off;
    desc.data_size   = num_rows * 8u;  // 24 — exceeds actual payload
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeUInt64>();
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── Bounds check: per-row wire offset points beyond data block ────────────────
//
// COL_BYTES: the offsets array is valid, but wire_offsets[1] = 100, which
// is larger than data_size = 5 → must throw WASM_ERROR.

TEST(ColumnBinaryWire, BoundsCheckWireOffsetOutOfRange)
{
    const uint32_t num_rows    = 1;
    const uint32_t offsets_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint32_t data_off    = offsets_off + (num_rows + 1u) * 4u;
    const uint32_t data_size   = 5u;

    std::vector<uint8_t> buf(data_off + data_size, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type           = COL_BYTES;
    desc.offsets_offset = offsets_off;
    desc.data_offset    = data_off;
    desc.data_size      = data_size;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    uint32_t wire_offs[2] = {0, 100};  // 100 >> data_size (5)
    std::memcpy(buf.data() + offsets_off, wire_offs, 8);

    auto result_type = std::make_shared<DataTypeString>();
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── COL_COMPLEX bounds: array outer offsets overflow buffer ───────────────────
//
// COL_COMPLEX describing Array(UInt64) for 3 rows but the data section is
// truncated so there is no room for the 4 outer uint32 offsets.

TEST(ColumnBinaryWire, BoundsCheckComplexArrayOffsetsOverflow)
{
    const uint32_t num_rows = 3;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint32_t data_size = 4u;  // only 4 bytes — too small for (3+1)×4 = 16 offset bytes

    std::vector<uint8_t> buf(data_off + data_size, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = data_size;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── COL_COMPLEX bounds: string chars overflow buffer ─────────────────────────
//
// COL_COMPLEX describing Array(String) for 1 row with 1 element.
// Outer uint64 offsets {0,1} claim 1 element; inner string offsets {0,9999}
// claim 9999 chars that don't exist in the buffer → must throw.

TEST(ColumnBinaryWire, BoundsCheckComplexStringCharsOverflow)
{
    // Sequential layout: outer uint64[2] + inner uint64[2], no char bytes
    const uint32_t num_rows  = 1;
    const uint32_t data_off  = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint32_t data_size = 2u * 8u + 2u * 8u;  // outer[2] + inner[2], uint64 each

    std::vector<uint8_t> buf(data_off + data_size, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = data_size;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    // outer uint64 offsets: {0, 1} — 1 string element in row 0
    uint64_t outer[2] = {0, 1};
    std::memcpy(buf.data() + data_off, outer, 16);

    // inner uint64 offsets: {0, 9999} — claims 9999 chars, none exist
    uint64_t inner[2] = {0, 9999};
    std::memcpy(buf.data() + data_off + 16, inner, 16);

    auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── COL_COMPLEX bounds: fixed-width data overflow buffer ─────────────────────
//
// COL_COMPLEX describing Tuple(UInt64, UInt64) for 2 rows.
// First field (UInt64, 2×8 = 16 bytes) fits; second field claims another
// 16 bytes that don't exist in the truncated buffer.

TEST(ColumnBinaryWire, BoundsCheckComplexFixedDataOverflow)
{
    const uint32_t num_rows  = 2;
    const uint32_t data_off  = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint32_t data_size = 16u;  // room for only 1 field (2×8), not 2

    std::vector<uint8_t> buf(data_off + data_size, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = data_size;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    DataTypes fields = {std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()};
    auto result_type = std::make_shared<DataTypeTuple>(fields);
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── COL_COMPLEX bounds: array outer offset not monotonic ──────────────────────

TEST(ColumnBinaryWire, BoundsCheckComplexArrayOffsetNotMonotonic)
{
    const uint32_t num_rows = 2;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    // Array(UInt64) for 2 rows: outer_offsets = [0, 3, 1] (not monotonic: 1 < 3)
    // total_elems = outer_offs[2] = 1
    std::vector<uint8_t> buf(data_off + 3 * 4);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = 3 * 4;  // 3 uint32 offsets
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    // outer_offsets: [0, 3, 1] — non-monotonic
    uint32_t * outer_offs = reinterpret_cast<uint32_t *>(buf.data() + data_off);
    outer_offs[0] = 0;
    outer_offs[1] = 3;
    outer_offs[2] = 1;

    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, arr_type, num_rows), DB::Exception);
}

// ── COL_COMPLEX bounds: array offset exceeds total_elems ──────────────────────

TEST(ColumnBinaryWire, BoundsCheckComplexArrayOffsetExceedsTotal)
{
    const uint32_t num_rows = 2;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    // Array(UInt64) for 2 rows: outer_offsets = [0, 2, 5] (off[1]=2 > total=5? No, off[2]=5 > off[1]=2)
    // Actually: off[2]=5 is total_elems, off[1]=2 should be <= 5 — that's fine.
    // Let's make off[1]=10 > total=5.
    std::vector<uint8_t> buf(data_off + 3 * 4);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = 3 * 4;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    uint32_t * outer_offs = reinterpret_cast<uint32_t *>(buf.data() + data_off);
    outer_offs[0] = 0;
    outer_offs[1] = 10;  // exceeds total_elems = 5
    outer_offs[2] = 5;

    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, arr_type, num_rows), DB::Exception);
}

// ── COL_COMPLEX bounds: string offset not monotonic ──────────────────────────

TEST(ColumnBinaryWire, BoundsCheckComplexStringOffsetNotMonotonic)
{
    const uint32_t num_rows = 1;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    // Array(String) for 1 row: wire_offsets = [0, 4, 2] (not monotonic: 2 < 4)
    // total_chars = wire_offs[1] = 2
    std::vector<uint8_t> buf(data_off + 3 * 4 + 4);  // offsets + 4 chars
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = 3 * 4 + 4;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    uint32_t * wire_offs = reinterpret_cast<uint32_t *>(buf.data() + data_off);
    wire_offs[0] = 0;
    wire_offs[1] = 4;
    wire_offs[2] = 2;  // not monotonic

    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, arr_type, num_rows), DB::Exception);
}

// ── COL_COMPLEX bounds: string offset exceeds total_chars ────────────────────

TEST(ColumnBinaryWire, BoundsCheckComplexStringOffsetExceedsTotal)
{
    const uint32_t num_rows = 1;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    // Array(String) for 1 row: wire_offsets = [0, 10, 2] (off[1]=10 > total=2)
    std::vector<uint8_t> buf(data_off + 3 * 4 + 2);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = 3 * 4 + 2;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    uint32_t * wire_offs = reinterpret_cast<uint32_t *>(buf.data() + data_off);
    wire_offs[0] = 0;
    wire_offs[1] = 10;  // exceeds total_chars = 2
    wire_offs[2] = 2;

    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, arr_type, num_rows), DB::Exception);
}

// ── COL_COMPLEX bounds: total_elems exceeds available data ────────────────────

TEST(ColumnBinaryWire, BoundsCheckComplexTotalElemsExceedsData)
{
    const uint32_t num_rows = 1;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    // Array(UInt64) for 1 row: outer_offsets = [0, 0xFFFFFFFF]
    // total_elems = outer_offs[n] = 0xFFFFFFFF — huge, but only 4 bytes of actual data
    std::vector<uint8_t> buf(data_off + 2 * 4);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = 2 * 4;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    uint32_t * outer_offs = reinterpret_cast<uint32_t *>(buf.data() + data_off);
    outer_offs[0] = 0;
    outer_offs[1] = 0xFFFFFFFF;

    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, arr_type, num_rows), DB::Exception);
}

// ── COL_COMPLEX bounds: data_end constrained to data_size, not buf.size() ─────

TEST(ColumnBinaryWire, BoundsCheckComplexDataEndTruncated)
{
    const uint32_t num_rows = 1;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    // Buffer has 100 bytes but data_size says only 4 bytes of complex data.
    // The decoder must not read beyond data_size.
    std::vector<uint8_t> buf(100, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_COMPLEX;
    desc.data_offset = data_off;
    desc.data_size   = 4;  // only 4 bytes of complex data (1 uint32)
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    // Write a UInt64 at offset 4 — but data_size only allows up to 4+4=8,
    // and we need 8 bytes for one UInt64. This should fail.
    uint32_t val = 42;
    std::memcpy(buf.data() + data_off + 4, &val, 4);

    auto arr_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, arr_type, num_rows), DB::Exception);
}

// ── Malformed nullable descriptor: COL_IS_NULLABLE set but null_offset == 0 ──
//
// null_offset == 0 must never be treated as "no null map" for a nullable
// descriptor (byte 0 is always occupied by the frame header, so it can never
// be a real null-map location) — otherwise the decoder would silently return
// a plain (non-nullable) column for a declared Nullable(T) result, which can
// reach undefined behavior downstream (e.g. ColumnNullable::insertRangeFrom's
// release-build assert_cast) instead of a clean parse error.

TEST(ColumnBinaryWire, NullableDescriptorMissingNullMapRejected)
{
    const uint32_t num_rows = 3;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(data_off + num_rows * 8u, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED64 | COL_IS_NULLABLE;
    desc.null_offset = 0;  // malformed: nullable bit set but no null map
    desc.data_offset = data_off;
    desc.data_size   = num_rows * 8u;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── Malformed nullable descriptor: COL_IS_NULLABLE set but declared type isn't
// Nullable ────────────────────────────────────────────────────────────────────
//
// Must reject with a normal DB::Exception, not let a reference dynamic_cast
// throw std::bad_cast (which callers expecting this function's clean
// parse-error contract would not handle the same way).

TEST(ColumnBinaryWire, NullableBitAgainstNonNullableDeclaredTypeRejected)
{
    const uint32_t num_rows = 3;
    const uint32_t null_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint32_t data_off = null_off + num_rows;

    std::vector<uint8_t> buf(data_off + num_rows * 8u, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED64 | COL_IS_NULLABLE;
    desc.null_offset = null_off;
    desc.data_offset = data_off;
    desc.data_size   = num_rows * 8u;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeUInt64>();  // not Nullable, but wire says it is
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows),
                 DB::Exception);
}

// ── LowCardinality(String) encoder/decoder: verify COL_LOWCARD wire shape ────
//
// Input: 4 rows — "a", "b", "a", "c". ColumnUnique reserves position 0 for the
// implicit default (empty string) for String dictionaries, so dict_row_count
// ends up as 4: "" (default), "a", "b", "c".

TEST(ColumnBinaryWire, LowCardinalityStringEncodeDecodeRoundTrip)
{
    auto lowcard_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto lc_col = lowcard_type->createColumn();

    auto full = ColumnString::create();
    full->insertData("a", 1);
    full->insertData("b", 1);
    full->insertData("a", 1);
    full->insertData("c", 1);

    typeid_cast<ColumnLowCardinality &>(*lc_col).insertRangeFromFullColumn(*full, 0, full->size());

    uint32_t num_rows = 4;
    auto buf = encodeCHColumn(lc_col.get(), num_rows);
    ColDescriptor desc = readDesc(buf);
    EXPECT_EQ(desc.type, COL_LOWCARD);
    EXPECT_EQ(desc.null_offset, 0u);  // unused: nullability lives inside the dictionary type

    const uint8_t * header = buf.data() + desc.data_offset;
    uint32_t dict_row_count = 0;
    std::memcpy(&dict_row_count, header, 4);
    uint8_t index_elem_width = header[4];
    EXPECT_EQ(dict_row_count, 4u);
    EXPECT_EQ(index_elem_width, 1u);  // small dictionary fits in a UInt8 index

    // Index array at offsets_offset: one byte per row, referencing dictionary positions.
    const uint8_t * idx = buf.data() + desc.offsets_offset;
    EXPECT_EQ(idx[0], idx[2]);  // rows 0 and 2 are both "a" -> same dictionary position
    EXPECT_NE(idx[0], idx[1]);  // "a" != "b"
    EXPECT_NE(idx[0], idx[3]);  // "a" != "c"

    auto decoded = readColumnarOutput({buf.data(), buf.size()}, lowcard_type, num_rows);
    const auto * decoded_lc = typeid_cast<const ColumnLowCardinality *>(decoded.get());
    ASSERT_NE(decoded_lc, nullptr);
    ASSERT_EQ(decoded_lc->size(), 4u);
    EXPECT_EQ(decoded_lc->getDataAt(0), "a");
    EXPECT_EQ(decoded_lc->getDataAt(1), "b");
    EXPECT_EQ(decoded_lc->getDataAt(2), "a");
    EXPECT_EQ(decoded_lc->getDataAt(3), "c");
}

// ── Bounds check: wrapped offset must not bypass the check via integer overflow ──
//
// A malformed frame can set an offset field close to UINT64_MAX. The naive check
// `offset + length > buf.size()` overflows the addition and wraps back into range,
// trivially passing. The safe form checks `offset > buf.size()` first, then bounds
// the length against the *remaining* space. These three cases exercise that pattern
// at the null map, COL_BYTES offsets array, and COL_FIXED8 data sites.

TEST(ColumnBinaryWire, BoundsCheckWrappedNullOffsetRejected)
{
    const uint32_t num_rows = 3;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(data_off + num_rows * 8u, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED64 | COL_IS_NULLABLE;
    // null_offset + num_rows would wrap a naive uint64_t addition back under buf.size().
    desc.null_offset = std::numeric_limits<uint64_t>::max() - 1;
    desc.data_offset = data_off;
    desc.data_size   = num_rows * 8u;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows), DB::Exception);
}

TEST(ColumnBinaryWire, BoundsCheckWrappedColBytesOffsetsOffsetRejected)
{
    const uint32_t num_rows  = 1;
    const uint32_t data_off  = FRAME_HEADER_BYTES + COL_DESC_BYTES;
    const uint32_t data_size = 1u;

    std::vector<uint8_t> buf(data_off + data_size, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type = COL_BYTES;
    // offsets_offset + (num_rows + 1) * 8 would wrap a naive uint64_t addition back
    // under buf.size().
    desc.offsets_offset = std::numeric_limits<uint64_t>::max() - 4;
    desc.data_offset    = data_off;
    desc.data_size      = data_size;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeString>();
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows), DB::Exception);
}

TEST(ColumnBinaryWire, BoundsCheckWrappedLowCardIndexOffsetsOffsetRejected)
{
    const uint32_t num_rows = 4;
    constexpr uint64_t header_bytes = 4u + 4u + COL_DESC_BYTES;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(data_off + header_bytes, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    // Minimal COL_LOWCARD header: dict_row_count=1, index_elem_width=1, empty dict_desc
    // (COL_BYTES, 0 rows, 0 bytes) so the dictionary decode itself succeeds trivially.
    uint8_t * header = buf.data() + data_off;
    uint32_t dict_row_count = 1;
    std::memcpy(header, &dict_row_count, 4);
    header[4] = 1;  // index_elem_width
    ColDescriptor dict_desc{};
    dict_desc.type        = COL_BYTES;
    dict_desc.offsets_offset = data_off;  // 0-row COL_BYTES: offsets array is [0], within region
    dict_desc.data_offset = data_off;
    dict_desc.data_size   = 0;
    std::memcpy(header + 8u, &dict_desc, COL_DESC_BYTES);

    ColDescriptor desc{};
    desc.type = COL_LOWCARD;
    // offsets_offset + num_rows * index_elem_width would wrap a naive uint64_t addition
    // back under buf.size(), letting the index array read run past the frame.
    desc.offsets_offset = std::numeric_limits<uint64_t>::max() - 2;
    desc.data_offset = data_off;
    desc.data_size   = header_bytes;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows), DB::Exception);
}

// ── COL_FIXED64 must reject a data_size that doesn't match the row count ─────
//
// desc.data_size claims 1 byte, but rows_to_dec (=num_rows) is 3, so the actual
// memcpy would need 24 bytes. Without cross-checking data_size against
// rows_to_dec * width, the buf.size()-relative bounds check alone would let this
// read run into whatever bytes happen to follow within the frame.

TEST(ColumnBinaryWire, BoundsCheckFixed64DataSizeMismatchRejected)
{
    const uint32_t num_rows = 3;
    const uint32_t data_off = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(data_off + num_rows * 8u, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED64;
    desc.data_offset = data_off;
    desc.data_size   = 1;  // should be num_rows * 8 = 24
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeUInt64>();
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows), DB::Exception);
}

// ── readColumnarOutput must reject descriptors pointing into header/descriptor
// metadata, matching ColumnBinaryInputFormat's equivalent check ────────────────
//
// data_offset = 0 points at the frame header instead of a real data section;
// without this check, readColumnFromDesc would silently decode header bytes as
// the column's payload instead of throwing.

TEST(ColumnBinaryWire, OutputRejectsDataOffsetInsideHeader)
{
    const uint32_t num_rows = 1;
    const uint32_t hdr_desc_size = FRAME_HEADER_BYTES + COL_DESC_BYTES;

    std::vector<uint8_t> buf(hdr_desc_size, 0);
    writeFrameHeader(buf.data(), num_rows, /*num_cols=*/1);

    ColDescriptor desc{};
    desc.type        = COL_FIXED64;
    desc.data_offset = 0;  // points at the frame header, not a real data section
    desc.data_size   = 8;
    std::memcpy(buf.data() + FRAME_HEADER_BYTES, &desc, COL_DESC_BYTES);

    auto result_type = std::make_shared<DataTypeUInt64>();
    EXPECT_THROW(readColumnarOutput({buf.data(), buf.size()}, result_type, num_rows), DB::Exception);
}
