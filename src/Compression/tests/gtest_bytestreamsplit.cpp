#include <Compression/CompressionFactory.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Common/PODArray.h>

#include <Compression/ICompressionCodec.h>

#include <cstring>
#include <cmath>
#include <algorithm>
#include <initializer_list>
#include <vector>
#include <random>

#include <gtest/gtest.h>

using namespace DB;

// ─── Helpers ──────────────────────────────────────────────────────────────────

namespace
{

CompressionCodecPtr makeCodec(const std::string & spec, const DataTypePtr & type = nullptr)
{
    const std::string stmt = "(" + spec + ")";
    Tokens toks(stmt.begin().base(), stmt.end().base());
    IParser::Pos pos(toks, 0, 0);
    Expected exp;
    ASTPtr ast;
    ParserCodec{}.parse(pos, ast, exp);
    return CompressionCodecFactory::instance().get(ast, type);
}

// Round-trip helper: compress then decompress, assert byte-exact equality.
void roundTrip(ICompressionCodec & codec,
               const std::vector<char> & source,
               const std::string & label = "")
{
    const UInt32 src_size = static_cast<UInt32>(source.size());

    const UInt32 reserve = codec.getCompressedReserveSize(src_size);
    PODArray<char> compressed(reserve);

    ASSERT_GT(src_size, 0u) << label;

    const UInt32 comp_size = codec.compress(source.data(), src_size, compressed.data());
    compressed.resize(comp_size);

    PODArray<char> decoded(src_size);
    const UInt32 dec_size = codec.decompress(compressed.data(), comp_size, decoded.data());
    decoded.resize(dec_size);

    ASSERT_EQ(dec_size, src_size) << label;
    ASSERT_EQ(0, memcmp(source.data(), decoded.data(), src_size))
        << label << ": decoded bytes differ from original";
}

// Build a vector<char> from an initializer_list of typed values (little-endian store).
template <typename T>
std::vector<char> makeBuffer(std::initializer_list<T> vals)
{
    std::vector<char> buf(sizeof(T) * vals.size());
    char * p = buf.data();
    for (T v : vals)
    {
        memcpy(p, &v, sizeof(T));
        p += sizeof(T);
    }
    return buf;
}

// Generate N values via a generator functor, stored as T.
template <typename T, typename Gen>
std::vector<char> generateBuffer(size_t n, Gen gen)
{
    std::vector<char> buf(sizeof(T) * n);
    char * p = buf.data();
    for (size_t i = 0; i < n; ++i)
    {
        T v = static_cast<T>(gen(i));
        memcpy(p, &v, sizeof(T));
        p += sizeof(T);
    }
    return buf;
}

} // namespace


// ─── Parameterized roundtrip suite ────────────────────────────────────────────
//
// Mirrors the DoubleDelta/Gorilla parameterized approach in gtest_compressionCodec.cpp.
// We test ByteStreamSplit standalone (no chained compressor) because we only care
// about the byte-transpose correctness here; compressor interaction is tested in SQL.

struct BSSParam
{
    std::string codec_spec;
    DataTypePtr data_type;
    std::vector<char> data;
    std::string label;
};

class ByteStreamSplitRoundtripTest : public ::testing::TestWithParam<BSSParam>
{};

TEST_P(ByteStreamSplitRoundtripTest, RoundTrip)
{
    const auto & p = GetParam();
    auto codec = makeCodec(p.codec_spec, p.data_type);
    roundTrip(*codec, p.data, p.label);
}

// ─── Codec-spec × data combinations ──────────────────────────────────────────

static std::vector<BSSParam> makeBSSParams()
{
    std::vector<BSSParam> out;

    // Float32 / W=4
    {
        auto dt = std::make_shared<DataTypeFloat32>();
        const std::string spec = "ByteStreamSplit(4)";

        // constant value
        out.push_back({spec, dt,
            generateBuffer<float>(1000, [](size_t) { return 2.718281828f; }),
            "F32_constant"});

        // sequential (slowly growing — exponent bytes stay the same)
        out.push_back({spec, dt,
            generateBuffer<float>(1000, [](size_t i) { return static_cast<float>(i) * 0.001f; }),
            "F32_sequential"});

        // pseudorandom (worst case)
        out.push_back({spec, dt,
            generateBuffer<float>(1000, [](size_t i) { return std::sin(static_cast<float>(i * i * i)); }),
            "F32_random"});

        // special values: ±0, ±inf, NaN, min/max
        out.push_back({spec, dt,
            makeBuffer<float>({0.0f, -0.0f,
                               std::numeric_limits<float>::infinity(),
                               -std::numeric_limits<float>::infinity(),
                               std::numeric_limits<float>::quiet_NaN(),
                               std::numeric_limits<float>::min(),
                               std::numeric_limits<float>::max(),
                               std::numeric_limits<float>::lowest(),
                               std::numeric_limits<float>::epsilon()}),
            "F32_specials"});

        // N=1 (boundary: single element)
        out.push_back({spec, dt, makeBuffer<float>({3.14f}), "F32_N1"});

        // N=2 (boundary)
        out.push_back({spec, dt, makeBuffer<float>({1.0f, 2.0f}), "F32_N2"});

        // N just below SIMD block boundary (31, 15)
        out.push_back({spec, dt,
            generateBuffer<float>(31, [](size_t i) { return static_cast<float>(i); }),
            "F32_N31"});
        out.push_back({spec, dt,
            generateBuffer<float>(33, [](size_t i) { return static_cast<float>(i); }),
            "F32_N33"});
    }

    // Float64 / W=8
    {
        auto dt = std::make_shared<DataTypeFloat64>();
        const std::string spec = "ByteStreamSplit(8)";

        out.push_back({spec, dt,
            generateBuffer<double>(1000, [](size_t) { return 2.718281828459045; }),
            "F64_constant"});

        out.push_back({spec, dt,
            generateBuffer<double>(1000, [](size_t i) { return static_cast<double>(i) * 1e-6; }),
            "F64_sequential"});

        out.push_back({spec, dt,
            generateBuffer<double>(1000, [](size_t i) { return std::sin(static_cast<double>(i) * static_cast<double>(i)); }),
            "F64_random"});

        out.push_back({spec, dt,
            makeBuffer<double>({0.0, -0.0,
                                std::numeric_limits<double>::infinity(),
                                -std::numeric_limits<double>::infinity(),
                                std::numeric_limits<double>::quiet_NaN(),
                                std::numeric_limits<double>::min(),
                                std::numeric_limits<double>::max(),
                                std::numeric_limits<double>::lowest()}),
            "F64_specials"});

        // tail: N not divisible by SIMD block size
        out.push_back({spec, dt,
            generateBuffer<double>(103, [](size_t i) { return static_cast<double>(i); }),
            "F64_N103_prime"});
    }

    // Int16 / W=2
    {
        auto dt = std::make_shared<DataTypeInt16>();
        const std::string spec = "ByteStreamSplit(2)";

        out.push_back({spec, dt,
            generateBuffer<Int16>(1000, [](size_t i) { return static_cast<Int16>(i); }),
            "I16_sequential"});

        out.push_back({spec, dt,
            makeBuffer<Int16>({std::numeric_limits<Int16>::min(),
                               std::numeric_limits<Int16>::max(),
                               0, -1, 1}),
            "I16_extremes"});
    }

    // Int32 / W=4
    {
        auto dt = std::make_shared<DataTypeInt32>();
        const std::string spec = "ByteStreamSplit(4)";

        out.push_back({spec, dt,
            generateBuffer<Int32>(1000, [](size_t i) { return static_cast<Int32>(i * i * i); }),
            "I32_cubic"});
    }

    // UInt64 / W=8
    {
        auto dt = std::make_shared<DataTypeUInt64>();
        const std::string spec = "ByteStreamSplit(8)";

        out.push_back({spec, dt,
            generateBuffer<UInt64>(1000, [](size_t i) { return static_cast<UInt64>(i); }),
            "U64_sequential"});

        out.push_back({spec, dt,
            makeBuffer<UInt64>({0ULL, std::numeric_limits<UInt64>::max(), 1ULL, 0xDEADBEEFCAFEBABEULL}),
            "U64_extremes"});
    }

    // FixedString(16) / W=16 (the SIMD-optimised path)
    {
        auto dt = std::make_shared<DataTypeFixedString>(16);
        const std::string spec = "ByteStreamSplit(16)";

        // 64 elements → exactly 2 SIMD blocks of 32 elements each (AVX2)
        std::mt19937 rng(42);
        auto randBuf = generateBuffer<uint8_t>(64 * 16,
            [&rng](size_t) -> uint8_t { return static_cast<uint8_t>(rng()); });
        out.push_back({spec, dt, randBuf, "FS16_aligned_64"});

        // 65 elements → 2 full blocks + 1-element scalar tail
        auto randBuf2 = generateBuffer<uint8_t>(65 * 16,
            [&rng](size_t) -> uint8_t { return static_cast<uint8_t>(rng()); });
        out.push_back({spec, dt, randBuf2, "FS16_aligned_plus_tail"});

        // 1 element → pure scalar path
        out.push_back({spec, dt,
            makeBuffer<uint8_t>({0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}),
            "FS16_N1"});
    }

    // FixedString(3) / W=3 → runtime path (no SIMD specialisation)
    {
        auto dt = std::make_shared<DataTypeFixedString>(3);
        const std::string spec = "ByteStreamSplit(3)";

        out.push_back({spec, dt,
            generateBuffer<uint8_t>(101 * 3, [](size_t i) { return static_cast<uint8_t>(i); }),
            "FS3_N101"});
    }

    return out;
}

INSTANTIATE_TEST_SUITE_P(
    ByteStreamSplit,
    ByteStreamSplitRoundtripTest,
    ::testing::ValuesIn(makeBSSParams()),
    [](const ::testing::TestParamInfo<BSSParam> & param_info) { return param_info.param.label; }
);


// ─── TranscodeRawInput: buffer sizes 1..512, like DoubleDelta/T64 tests ───────
//
// Verifies that the codec handles every possible input size (including sizes
// that produce a bytes_to_skip tail) without any assertion or memory error.

TEST(ByteStreamSplitTest, TranscodeRawInput)
{
    // (type, element_bytes) pairs to exercise different dispatch paths:
    //   W=2/4/8        → compile-time encodeW<W>/decodeW<W>
    //   W=16           → SSE2/AVX2 specialisation in encodeW<16>/decodeW<16>
    //   W=3 (FS3)      → runtime-width loop in encodeRuntime/decodeRuntime
    // Combined with the buf_size 1..512 sweep, this drives bytes_to_skip != 0
    // through every encode/decode path the codec implements.
    const std::vector<std::pair<DataTypePtr, size_t>> kTypes = {
        {std::make_shared<DataTypeFloat32>(),     4},
        {std::make_shared<DataTypeFloat64>(),     8},
        {std::make_shared<DataTypeInt16>(),       2},
        {std::make_shared<DataTypeInt32>(),       4},
        {std::make_shared<DataTypeInt64>(),       8},
        {std::make_shared<DataTypeUInt16>(),      2},
        {std::make_shared<DataTypeUInt32>(),      4},
        {std::make_shared<DataTypeUInt64>(),      8},
        {std::make_shared<DataTypeFixedString>(3),  3},
        {std::make_shared<DataTypeFixedString>(16), 16},
    };

    for (auto & [type, elem_bytes] : kTypes)
    {
        auto codec = makeCodec("ByteStreamSplit", type);

        // Test buffer sizes 1 to 512 bytes (includes sizes not divisible by elem_bytes)
        for (size_t buf_size = 1; buf_size <= 512; ++buf_size)
        {
            std::vector<char> src(buf_size);
            for (size_t i = 0; i < buf_size; ++i)
                src[i] = static_cast<char>(i & 0xFF);

            const UInt32 src_sz = static_cast<UInt32>(buf_size);
            const UInt32 reserve = codec->getCompressedReserveSize(src_sz);

            PODArray<char> compressed(reserve);
            const UInt32 comp_sz = codec->compress(src.data(), src_sz, compressed.data());
            compressed.resize(comp_sz);

            PODArray<char> decoded(buf_size);
            const UInt32 dec_sz = codec->decompress(compressed.data(), comp_sz, decoded.data());
            decoded.resize(dec_sz);

            ASSERT_EQ(dec_sz, src_sz)
                << "type=" << type->getName() << " buf_size=" << buf_size;
            ASSERT_EQ(0, memcmp(src.data(), decoded.data(), buf_size))
                << "type=" << type->getName() << " buf_size=" << buf_size;
        }
    }
}


// ─── Malformed-input rejection ────────────────────────────────────────────────
//
// ByteStreamSplit is a pure byte permutation, so a valid encoded block always
// has codec-body size == HEADER_SIZE + uncompressed_size. doDecompressData
// must reject any source size that violates this invariant, otherwise extra
// trailing bytes are silently discarded and a truncated source produces
// out-of-bounds behaviour or partial output.

namespace
{

// Layout written by ICompressionCodec::compress:
//   [0]    method byte
//   [1..4] compressed_block_size_with_header  (LE UInt32)
//   [5..8] decompressed_size                  (LE UInt32)
//   [9..]  codec body
constexpr UInt32 kBlockHeaderSize = 9;

void overwriteLE(char * p, UInt32 v) { memcpy(p, &v, sizeof(v)); }

} // namespace

TEST(ByteStreamSplitTest, MalformedExtraTrailingBody)
{
    auto codec = makeCodec("ByteStreamSplit(4)", std::make_shared<DataTypeFloat32>());

    auto src = generateBuffer<float>(64, [](size_t i) { return static_cast<float>(i); });
    const UInt32 src_sz = static_cast<UInt32>(src.size());

    PODArray<char> compressed(codec->getCompressedReserveSize(src_sz));
    const UInt32 comp_sz = codec->compress(src.data(), src_sz, compressed.data());

    constexpr UInt32 kJunk = 17;
    PODArray<char> tampered(comp_sz + kJunk);
    memcpy(tampered.data(), compressed.data(), comp_sz);
    memset(tampered.data() + comp_sz, 0xAA, kJunk);
    overwriteLE(tampered.data() + 1, comp_sz + kJunk);

    PODArray<char> decoded(src_sz);
    EXPECT_THROW(
        codec->decompress(tampered.data(), comp_sz + kJunk, decoded.data()),
        DB::Exception);
}

TEST(ByteStreamSplitTest, MalformedTruncatedBody)
{
    auto codec = makeCodec("ByteStreamSplit(8)", std::make_shared<DataTypeFloat64>());

    auto src = generateBuffer<double>(32, [](size_t i) { return static_cast<double>(i); });
    const UInt32 src_sz = static_cast<UInt32>(src.size());

    PODArray<char> compressed(codec->getCompressedReserveSize(src_sz));
    const UInt32 comp_sz = codec->compress(src.data(), src_sz, compressed.data());

    constexpr UInt32 kMissing = 9;
    ASSERT_GT(comp_sz, kBlockHeaderSize + kMissing);
    const UInt32 truncated_sz = comp_sz - kMissing;
    overwriteLE(compressed.data() + 1, truncated_sz);

    PODArray<char> decoded(src_sz);
    EXPECT_THROW(
        codec->decompress(compressed.data(), truncated_sz, decoded.data()),
        DB::Exception);
}


// ─── Verify codec description is stored correctly ─────────────────────────────

TEST(ByteStreamSplitTest, CodecDescription)
{
    auto descStr = [](const CompressionCodecPtr & c) -> std::string
    {
        if (auto ast = c->getCodecDesc())
            return ast->formatForErrorMessage();
        return {};
    };

    // Without explicit width: falls back to default (4)
    {
        auto codec = makeCodec("ByteStreamSplit");
        const std::string s = descStr(codec);
        EXPECT_NE(s.find("ByteStreamSplit"), std::string::npos) << s;
    }

    // With explicit width 8
    {
        auto codec = makeCodec("ByteStreamSplit(8)");
        const std::string s = descStr(codec);
        EXPECT_NE(s.find("ByteStreamSplit"), std::string::npos) << s;
    }
}
