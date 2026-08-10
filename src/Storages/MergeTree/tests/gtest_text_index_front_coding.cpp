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
    extern const int CORRUPTED_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

using TokensFormat = TextIndexSerialization::TokensFormat;

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

std::vector<std::string> deserializeFrontCodedTokens(const std::string & buffer)
{
    ReadBufferFromMemory in(buffer.data(), buffer.size());
    auto [column, format] = TextIndexSerialization::deserializeTokens(in);

    EXPECT_EQ(format, static_cast<UInt64>(TokensFormat::FrontCodedStrings));

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
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
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
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
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
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
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
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
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
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
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
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
    writeVarUInt(1, out); /// num_tokens
    writeVarUInt(SerializationString::MAX_STRING_SIZE + 1, out);

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::TOO_LARGE_STRING_SIZE);
}

/// A reconstructed token size that does not overflow but exceeds the size cap must be rejected before allocating.
TEST(TextIndexFrontCoding, ReconstructedTokenTooLargeIsRejected)
{
    WriteBufferFromOwnString out;
    writeVarUInt(static_cast<UInt64>(TokensFormat::FrontCodedStrings), out);
    writeVarUInt(2, out); /// num_tokens

    writeVarUInt(4, out);
    out.write("test", 4);
    writeFrontCodedToken(out, /*lcp=*/ 4, /*data_size=*/ SerializationString::MAX_STRING_SIZE, "");

    EXPECT_EQ(deserializeTokensErrorCode(out.str()), ErrorCodes::TOO_LARGE_STRING_SIZE);
}
