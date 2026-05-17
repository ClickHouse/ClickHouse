#include <Functions/MD5IsaL.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{
namespace
{

struct MD5Case
{
    std::string input;
    std::string_view expected_hex;
};

std::string repeatString(std::string_view value, size_t count)
{
    std::string result;
    result.reserve(value.size() * count);

    for (size_t i = 0; i < count; ++i)
        result.append(value);

    return result;
}

std::string digestHex(const ColumnFixedString::Chars & output, size_t row)
{
    static constexpr char alphabet[] = "0123456789ABCDEF";

    std::string hex(MD5IsaL::digest_size * 2, '\0');
    const UInt8 * digest = &output[row * MD5IsaL::digest_size];

    for (size_t byte = 0; byte < MD5IsaL::digest_size; ++byte)
    {
        const auto value = static_cast<uint8_t>(digest[byte]);
        hex[byte * 2] = alphabet[value >> 4];
        hex[byte * 2 + 1] = alphabet[value & 0x0F];
    }

    return hex;
}

ColumnFixedString::Chars applyRows(const std::vector<std::string> & rows, bool & used_isal)
{
    ColumnFixedString::Chars output;
    output.resize(rows.size() * MD5IsaL::digest_size);

    auto get_row = [&](size_t row, const UInt8 *& begin, size_t & size)
    {
        begin = reinterpret_cast<const UInt8 *>(rows[row].data());
        size = rows[row].size();
    };

    used_isal = MD5IsaL::tryApply(rows.size(), output, get_row);
    return output;
}

void assertDigests(const std::vector<MD5Case> & cases)
{
    std::vector<std::string> rows;
    rows.reserve(cases.size());

    for (const auto & test_case : cases)
        rows.push_back(test_case.input);

    bool used_isal = false;
    const auto output = applyRows(rows, used_isal);

    ASSERT_TRUE(used_isal);
    ASSERT_EQ(output.size(), cases.size() * MD5IsaL::digest_size);

    for (size_t row = 0; row < cases.size(); ++row)
        EXPECT_EQ(std::string(cases[row].expected_hex), digestHex(output, row)) << "row: " << row << ", size: " << cases[row].input.size();
}

}

TEST(MD5IsaL, AvailabilityMatchesBuildConfig)
{
#if USE_ISAL_CRYPTO
    EXPECT_TRUE(MD5IsaL::enabled);
#else
    EXPECT_FALSE(MD5IsaL::enabled);
#endif
}

TEST(MD5IsaL, EmptyBatchTouchesNothing)
{
    ColumnFixedString::Chars output;
    auto get_row = [](size_t, const UInt8 *&, size_t &) { FAIL() << "empty batch must not request rows"; };

    /// Whether tryApply claims the empty batch depends on the build, but in
    /// both builds it must not call into get_row and must leave the output empty:
    /// the enabled path short-circuits success, the disabled path lets the
    /// scalar fallback do the (also empty) work.
    EXPECT_EQ(MD5IsaL::enabled, MD5IsaL::tryApply(0, output, get_row));
    EXPECT_TRUE(output.empty());
}

TEST(MD5IsaL, SmallBatchFallsBackToScalar)
{
    if constexpr (!MD5IsaL::enabled)
    {
        GTEST_SKIP() << "ISA-L Crypto MD5 is not enabled for this build";
    }

    /// Batches that cannot fill even one SIMD lane must be rejected so the
    /// caller can use the cheaper scalar OpenSSL path.
    ASSERT_GT(MD5IsaL::min_batch_size, 1u);

    const std::vector<std::string> rows(MD5IsaL::min_batch_size - 1, "abc");
    ColumnFixedString::Chars output;
    output.resize(rows.size() * MD5IsaL::digest_size);
    std::fill(output.begin(), output.end(), static_cast<UInt8>(0xCD));

    auto get_row = [&](size_t row, const UInt8 *& begin, size_t & size)
    {
        begin = reinterpret_cast<const UInt8 *>(rows[row].data());
        size = rows[row].size();
    };

    EXPECT_FALSE(MD5IsaL::tryApply(rows.size(), output, get_row));

    /// Rejection must leave the output buffer untouched so the scalar fallback
    /// can fill it without first having to clear stale partial writes.
    for (const auto value : output)
        EXPECT_EQ(0xCDu, static_cast<unsigned>(value));
}

TEST(MD5IsaL, LargeBatchSpansMultipleWaves)
{
    if constexpr (!MD5IsaL::enabled)
    {
        GTEST_SKIP() << "ISA-L Crypto MD5 is not enabled for this build";
    }

    /// Sprinkle the known-answer input "abc" across multiple waves of the
    /// fixed-size lane pool to make sure wave boundaries do not scramble
    /// completion order or stomp on previously-written digests.
    constexpr std::string_view canonical_abc = "900150983CD24FB0D6963F7D28E17F72";
    const size_t row_count = MD5IsaL::lane_count * 3 + 5;

    std::vector<std::string> rows;
    rows.reserve(row_count);
    for (size_t row = 0; row < row_count; ++row)
        rows.push_back(repeatString(std::to_string(row), (row % 17) + 1));

    std::vector<size_t> abc_rows;
    for (size_t row = 0; row < row_count; row += MD5IsaL::lane_count - 1)
    {
        rows[row] = "abc";
        abc_rows.push_back(row);
    }

    bool used_isal = false;
    const auto output = applyRows(rows, used_isal);
    ASSERT_TRUE(used_isal);

    for (const auto row : abc_rows)
        EXPECT_EQ(std::string(canonical_abc), digestHex(output, row)) << "row: " << row;
}

TEST(MD5IsaL, HashesKnownBoundaryVectors)
{
    if constexpr (!MD5IsaL::enabled)
    {
        GTEST_SKIP() << "ISA-L Crypto MD5 is not enabled for this build";
    }

    assertDigests({
        {"", "D41D8CD98F00B204E9800998ECF8427E"},
        {"a", "0CC175B9C0F1B6A831C399E269772661"},
        {"abc", "900150983CD24FB0D6963F7D28E17F72"},
        {"message digest", "F96B697D7CB7938D525A2F31AAF161D0"},
        {"abcdefghijklmnopqrstuvwxyz", "C3FCD3D76192E4007DFB496CCA67E13B"},
        {std::string(55, 'x'), "04364420E25C512FD958A70738AA8F72"},
        {std::string(56, 'x'), "668A72D5BA17F08E62DABCAFAD6DB14B"},
        {std::string(63, 'x'), "7DC2CA208106A2F703567BDFF99D8981"},
        {std::string(64, 'x'), "C1BB4F81D892B2D57947682AEB252456"},
        {std::string(65, 'x'), "1BC932052302D074BDEC39795FE00CF6"},
        {std::string(127, 'x'), "A0B28C1DA68705C2FF883FE279B72753"},
        {std::string(128, 'x'), "D69CB61A6EE87200676EB0D4B90EDBCB"},
        {std::string(129, 'x'), "3926841D393C00C3F36260E5ACE10DC1"},
        {std::string(1000, 'x'), "398533D48111E9F664B1F64CB10C4B63"},
    });
}

TEST(MD5IsaL, KeepsRowOrderForMixedBatch)
{
    if constexpr (!MD5IsaL::enabled)
    {
        GTEST_SKIP() << "ISA-L Crypto MD5 is not enabled for this build";
    }

    const std::vector<std::string_view> expected_hex = {
        "CFCD208495D565EF66E7DFF9F98764DA", "E78582C7FA761CB9358009503F2810A9", "ED008F50F0D24FD54162C2749700958A",
        "50F8C0C582EB29449AC8D0E76C3B97A1", "7C56420B1A2379208C4FA04FA21EE246", "DD7A0FCD74C3A90DEC1C99E29FC11B06",
        "77E9B80BBE96DFC3B88E20B5376D62B9", "DB30DCB198A783F8BA4A55FB162D6627", "583B25111483B14CA58755A7775D02E9",
        "2DCAF6FEA5AF2E354A2AF333B576C87B", "D3D9446802A44259755D38E6D163E820", "2C3A51285D24FFB34B14E35399B01339",
        "A1147C86DB7D8B1501FF585E81E30089", "C3566D330C01FA9502C0854FB0145EE1", "45B0FC1E4A0B17B4B9D9E2244B4A99D1",
        "A844B5AD1ABD4288CA0D91D41E3306D7", "159AD629C1D32E2F16CA91FC4DC8D7CA", "CC3CE6437055488DE4F174DC97407318",
        "5AD594A294627155A603D05B9500DAB1", "CFE3A220546835073551E0074AC5AE15", "98F13708210194C475687BE6106A3B84",
        "E938F43DF2C4DB81B55E988C318228B2", "D9CB760A8F61BB2E54A0462DC2BD423B", "EBCA35E90BD3456234468A710649217C",
    };

    std::vector<MD5Case> cases;
    cases.reserve(expected_hex.size());

    for (size_t row = 0; row < expected_hex.size(); ++row)
        cases.push_back({repeatString(std::to_string(row), (row % 10) * 13 + 1), expected_hex[row]});

    assertDigests(cases);
}

TEST(MD5IsaL, UnsupportedBuildLeavesFallbackToCaller)
{
    if constexpr (MD5IsaL::enabled)
    {
        GTEST_SKIP() << "ISA-L Crypto MD5 is enabled for this build";
    }

    std::vector<std::string> rows = {"abc", std::string(64, 'x')};
    ColumnFixedString::Chars output;
    output.resize(rows.size() * MD5IsaL::digest_size);
    std::fill(output.begin(), output.end(), static_cast<UInt8>(0xAB));

    auto get_row = [&](size_t row, const UInt8 *& begin, size_t & size)
    {
        begin = reinterpret_cast<const UInt8 *>(rows[row].data());
        size = rows[row].size();
    };

    ASSERT_FALSE(MD5IsaL::tryApply(rows.size(), output, get_row));

    for (const auto value : output)
        EXPECT_EQ(0xABu, static_cast<unsigned>(value));
}

}
