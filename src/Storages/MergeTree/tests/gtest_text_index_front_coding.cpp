#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CORRUPTED_DATA;
}

namespace
{

/// Appends a front-coded token: common-prefix length, suffix size, suffix bytes.
void writeFrontCodedToken(WriteBuffer & out, UInt64 lcp, UInt64 data_size, std::string_view suffix)
{
    writeVarUInt(lcp, out);
    writeVarUInt(data_size, out);
    out.write(suffix.data(), suffix.size());
}

/// Returns the error code thrown while deserializing the dictionary block, or 0 if none.
int deserializeTokensErrorCode(const std::string & buffer)
{
    ReadBufferFromMemory in(buffer.data(), buffer.size());
    try
    {
        TextIndexSerialization::deserializeTokens(in);
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

std::vector<std::string> deserializeFrontCodedTokens(
    const std::string & buffer, TextIndexDictionaryBlockEncoding expected_encoding = TextIndexDictionaryBlockEncoding::FrontCodingV1)
{
    ReadBufferFromMemory in(buffer.data(), buffer.size());
    auto [column, format] = TextIndexSerialization::deserializeTokens(in);

    EXPECT_EQ(format, static_cast<UInt64>(expected_encoding));

    /// Cumulative offsets, no terminating zeros: token i is the range [offsets[i - 1], offsets[i]).
    const auto & string_column = assert_cast<const ColumnString &>(*column);
    const auto & chars = string_column.getChars();
    const auto & offsets = string_column.getOffsets();

    std::vector<std::string> tokens;
    size_t begin = 0;
    for (size_t i = 0; i < string_column.size(); ++i)
    {
        size_t end = offsets[i];
        tokens.emplace_back(reinterpret_cast<const char *>(chars.data()) + begin, end - begin);
        begin = end;
    }
    return tokens;
}

}

/// A valid block must round-trip without being rejected by the hardening checks.
TEST(TextIndexFrontCoding, ValidBlockIsAccepted)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(3, out); /// num_tokens

    /// First token is stored verbatim; the rest are front-coded against the previous one.
    writeVarUInt(3, out);
    out.write("car", 3);
    writeFrontCodedToken(out, /*lcp=*/ 3, /*data_size=*/ 1, "d");
    writeFrontCodedToken(out, /*lcp=*/ 3, /*data_size=*/ 1, "e");

    const auto tokens = deserializeFrontCodedTokens(out.str());
    ASSERT_EQ(tokens.size(), 3u);
    EXPECT_EQ(tokens[0], "car");
    EXPECT_EQ(tokens[1], "card");
    EXPECT_EQ(tokens[2], "care");
}

/// The exact scenario from the security report: 0xFFFFFFFFFFFFFF00 + 0x100 wraps to 0.
TEST(TextIndexFrontCoding, LcpPlusDataSizeOverflowIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(2, out); /// num_tokens

    writeVarUInt(4, out);
    out.write("test", 4);
    writeFrontCodedToken(out, /*lcp=*/ 0xFFFFFFFFFFFFFF00ULL, /*data_size=*/ 0x100ULL, "");

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

/// `lcp` larger than the previous token would make the memcpy copy an attacker-controlled length.
TEST(TextIndexFrontCoding, LcpLargerThanPreviousTokenIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(2, out); /// num_tokens

    writeVarUInt(4, out);
    out.write("test", 4);
    writeFrontCodedToken(out, /*lcp=*/ 100, /*data_size=*/ 0, "");

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

/// Valid `lcp`, but `data_size` so large that `lcp + data_size` overflows.
TEST(TextIndexFrontCoding, DataSizeOverflowIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(2, out); /// num_tokens

    writeVarUInt(4, out);
    out.write("test", 4);
    writeFrontCodedToken(out, /*lcp=*/ 4, /*data_size=*/ 0xFFFFFFFFFFFFFFFFULL, "");

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

/// `lcp + data_size` does not overflow, but adding it to the running `offset` does.
TEST(TextIndexFrontCoding, RunningOffsetOverflowIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(2, out); /// num_tokens

    writeVarUInt(4, out);
    out.write("test", 4);
    /// lcp (4) + data_size (0xFFFFFFFFFFFFFFFB) == 0xFFFFFFFFFFFFFFFF, but offset (4) + that overflows.
    writeFrontCodedToken(out, /*lcp=*/ 4, /*data_size=*/ 0xFFFFFFFFFFFFFFFBULL, "");

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

/// A first token size that does not overflow but exceeds the size cap must be rejected before allocating.
TEST(TextIndexFrontCoding, FirstTokenTooLargeIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(1, out); /// num_tokens
    writeVarUInt(SerializationString::MAX_STRING_SIZE + 1, out);

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

/// A reconstructed token size that does not overflow but exceeds the size cap must be rejected before allocating.
TEST(TextIndexFrontCoding, ReconstructedTokenTooLargeIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV1), out);
    writeVarUInt(2, out); /// num_tokens

    writeVarUInt(4, out);
    out.write("test", 4);
    writeFrontCodedToken(out, /*lcp=*/ 4, /*data_size=*/ SerializationString::MAX_STRING_SIZE, "");

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

namespace
{

/// Writes the header of a `frontcoding_v2` block, its first token and the bit widths of both length columns.
void writeFrontCodingV2Header(WriteBuffer & out, UInt64 num_tokens, std::string_view first_token, UInt64 lcp_bit_width, UInt64 suffix_size_bit_width)
{
    writeVarUInt(static_cast<UInt64>(TextIndexDictionaryBlockEncoding::FrontCodingV2), out);
    writeVarUInt(num_tokens, out);
    writeVarUInt(first_token.size(), out);
    out.write(first_token.data(), first_token.size());
    writeVarUInt(lcp_bit_width, out);
    writeVarUInt(suffix_size_bit_width, out);
}

}

TEST(TextIndexFrontCodingV2, RoundTrip)
{
    auto tokens = ColumnString::create();
    for (std::string_view token : {"car", "card", "care", "dog"})
        tokens->insertData(token.data(), token.size());

    WriteBufferFromOwnString out;
    TextIndexSerialization::serializeTokens(*tokens, out, TextIndexDictionaryBlockEncoding::FrontCodingV2);

    const auto result = deserializeFrontCodedTokens(out.str(), TextIndexDictionaryBlockEncoding::FrontCodingV2);
    EXPECT_EQ(result, (std::vector<std::string>{"car", "card", "care", "dog"}));
}

/// Lengths of tokens above 4 GiB need more than 32 bits.
TEST(TextIndexFrontCodingV2, BitWidthAbove32IsAccepted)
{
    WriteBufferFromOwnString out;
    writeFrontCodingV2Header(out, /*num_tokens=*/ 2, "ab", /*lcp_bit_width=*/ 33, /*suffix_size_bit_width=*/ 1);
    out.write("\x01\x00\x00\x00\x00", 5); /// lcp = 1 in 33 bits
    out.write("\x01", 1); /// suffix size = 1 in 1 bit
    out.write("c", 1);

    const auto result = deserializeFrontCodedTokens(out.str(), TextIndexDictionaryBlockEncoding::FrontCodingV2);
    EXPECT_EQ(result, (std::vector<std::string>{"ab", "ac"}));
}

TEST(TextIndexFrontCodingV2, BitWidthTooLargeIsRejected)
{
    WriteBufferFromOwnString out;
    writeFrontCodingV2Header(out, /*num_tokens=*/ 2, "ab", /*lcp_bit_width=*/ 64, /*suffix_size_bit_width=*/ 1);

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

/// (2^61 - 1) * 8 bits does not overflow, but rounding it up to whole words does.
TEST(TextIndexFrontCodingV2, BitColumnSizeOverflowIsRejected)
{
    WriteBufferFromOwnString out;
    writeFrontCodingV2Header(out, /*num_tokens=*/ 1ULL << 61, "a", /*lcp_bit_width=*/ 8, /*suffix_size_bit_width=*/ 8);
    out.write("\x00\x00\x00\x00", 4);

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}

TEST(TextIndexFrontCodingV2, TruncatedBitColumnIsRejected)
{
    WriteBufferFromOwnString out;
    writeFrontCodingV2Header(out, /*num_tokens=*/ 1000, "a", /*lcp_bit_width=*/ 8, /*suffix_size_bit_width=*/ 8);
    out.write("\x00\x00\x00\x00", 4);

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CANNOT_READ_ALL_DATA);
}

TEST(TextIndexFrontCodingV2, LcpLargerThanPreviousTokenIsRejected)
{
    WriteBufferFromOwnString out;
    writeFrontCodingV2Header(out, /*num_tokens=*/ 2, "ab", /*lcp_bit_width=*/ 2, /*suffix_size_bit_width=*/ 0);
    out.write("\x03", 1); /// lcp = 3

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::CORRUPTED_DATA);
}
