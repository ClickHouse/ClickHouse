#include <gtest/gtest.h>

#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Common/Exception.h>

#include <string_view>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
extern const int CANNOT_READ_ALL_DATA;
}

namespace
{

String readAll(ReadBuffer & in)
{
    String result;
    WriteBufferFromString out(result);
    copyData(in, out);
    out.finalize();
    return result;
}

int codeOfThrown(ReadBuffer & in)
{
    try
    {
        readAll(in);
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

}

TEST(LimitReadBuffer, StreamEndingAtTheLimitIsNotAnError)
{
    ReadBufferFromString nested(std::string_view("0123456789"));
    LimitReadBuffer limited(nested, {.read_no_more = 10, .expect_eof = true, .excetion_hint = "hint"});
    EXPECT_EQ(readAll(limited), "0123456789");
}

TEST(LimitReadBuffer, ExpectEofRejectsDataPastTheLimit)
{
    ReadBufferFromString nested(std::string_view("0123456789abc"));
    LimitReadBuffer limited(nested, {.read_no_more = 10, .expect_eof = true, .excetion_hint = "hint"});
    EXPECT_EQ(codeOfThrown(limited), ErrorCodes::LIMIT_EXCEEDED);
}

TEST(LimitReadBuffer, ZeroLimitRejectsANonEmptyStream)
{
    ReadBufferFromString nested(std::string_view("a"));
    LimitReadBuffer limited(nested, {.read_no_more = 0, .expect_eof = true, .excetion_hint = "hint"});
    EXPECT_EQ(codeOfThrown(limited), ErrorCodes::LIMIT_EXCEEDED);
}

TEST(LimitReadBuffer, WithoutExpectEofDataPastTheLimitIsCutOff)
{
    ReadBufferFromString nested(std::string_view("0123456789abc"));
    LimitReadBuffer limited(nested, {.read_no_more = 10});
    EXPECT_EQ(readAll(limited), "0123456789");
}

/// The check runs in `nextImpl`, so a consumer that reads exactly the limit and stops never asks for
/// the byte that would reveal the overflow. `expect_eof` is therefore best-effort, not a guarantee.
TEST(LimitReadBuffer, ExpectEofIsNotCheckedWhenTheConsumerStopsAtTheLimit)
{
    ReadBufferFromString nested(std::string_view("0123456789abc"));
    LimitReadBuffer limited(nested, {.read_no_more = 10, .expect_eof = true, .excetion_hint = "hint"});
    char buf[10] = {};
    limited.readStrict(buf, sizeof(buf));
    EXPECT_EQ(String(buf, sizeof(buf)), "0123456789");
}

TEST(LimitReadBuffer, ExpectEofShortStreamIsNotAnError)
{
    ReadBufferFromString nested(std::string_view("012"));
    LimitReadBuffer limited(nested, {.read_no_more = 10, .expect_eof = true, .excetion_hint = "hint"});
    EXPECT_EQ(readAll(limited), "012");
}

TEST(LimitReadBuffer, StreamShorterThanReadNoLessIsAnError)
{
    ReadBufferFromString nested(std::string_view("012"));
    LimitReadBuffer limited(nested, {.read_no_less = 10, .read_no_more = 10});
    EXPECT_EQ(codeOfThrown(limited), ErrorCodes::CANNOT_READ_ALL_DATA);
}

TEST(LimitReadBuffer, WithoutExpectEofStreamEndingAtTheLimitIsFine)
{
    ReadBufferFromString nested(std::string_view("0123456789"));
    LimitReadBuffer limited(nested, {.read_no_more = 10});
    EXPECT_EQ(readAll(limited), "0123456789");
}

TEST(LimitReadBuffer, WithoutExpectEofShortStreamIsFine)
{
    ReadBufferFromString nested(std::string_view("012"));
    LimitReadBuffer limited(nested, {.read_no_more = 10});
    EXPECT_EQ(readAll(limited), "012");
}

TEST(LimitReadBuffer, WithoutExpectEofEmptyStreamIsFine)
{
    ReadBufferFromString nested(std::string_view(""));
    LimitReadBuffer limited(nested, {.read_no_more = 10});
    EXPECT_EQ(readAll(limited), "");
}

TEST(LimitReadBuffer, WithoutExpectEofZeroLimitReadsNothing)
{
    ReadBufferFromString nested(std::string_view("abc"));
    {
        LimitReadBuffer limited(nested, {.read_no_more = 0});
        EXPECT_EQ(readAll(limited), "");
    }
    EXPECT_EQ(readAll(nested), "abc");
}

TEST(LimitReadBuffer, WithoutExpectEofNestedBufferContinuesPastTheLimit)
{
    ReadBufferFromString nested(std::string_view("0123456789abc"));
    {
        LimitReadBuffer limited(nested, {.read_no_more = 10});
        EXPECT_EQ(readAll(limited), "0123456789");
    }
    EXPECT_EQ(readAll(nested), "abc");
}

/// The `Content-Length` shape of `HTTPServerRequest`. With keep-alive the nested buffer holds the next
/// request, so the bytes past the limit have to stay readable.
TEST(LimitReadBuffer, ExactLengthLeavesTheRestForTheNextReader)
{
    ReadBufferFromString nested(std::string_view("0123456789abc"));
    {
        LimitReadBuffer limited(nested, {.read_no_less = 10, .read_no_more = 10});
        EXPECT_EQ(readAll(limited), "0123456789");
    }
    EXPECT_EQ(readAll(nested), "abc");
}

TEST(LimitReadBuffer, PartialReadLeavesNestedBufferAtTheConsumedOffset)
{
    ReadBufferFromString nested(std::string_view("0123456789abc"));
    {
        LimitReadBuffer limited(nested, {.read_no_more = 10});
        char buf[4] = {};
        limited.readStrict(buf, sizeof(buf));
        EXPECT_EQ(String(buf, sizeof(buf)), "0123");
    }
    EXPECT_EQ(readAll(nested), "456789abc");
}
