#include <gtest/gtest.h>

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/readIntText.h>
#include <iostream>


namespace DB::ErrorCodes
{
    extern const int OK;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

using namespace DB;

template <int base, typename T>
int getErrorOfParseIntInBase(std::string_view text)
{
    try
    {
        parseIntInBase<base, T>(text);
        return ErrorCodes::OK;
    }
    catch (Exception & e)
    {
        return e.code();
    }
}

TEST(ReadIntTextTest, parseIntInBase)
{
    EXPECT_EQ((parseIntInBase<2, int>("1101")), 13);
    EXPECT_EQ((parseIntInBase<8, int>("1101")), 577);
    EXPECT_EQ((parseIntInBase<10, int>("1101")), 1101);
    EXPECT_EQ((parseIntInBase<16, int>("1101")), 4353);

    EXPECT_EQ((getErrorOfParseIntInBase<2, int>("125")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<8, int>("125")), 85);
    EXPECT_EQ((parseIntInBase<10, int>("125")), 125);
    EXPECT_EQ((parseIntInBase<16, int>("125")), 293);

    EXPECT_EQ((getErrorOfParseIntInBase<2, int>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<8, int>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<10, int>("1F")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<16, int>("1F")), 31);

    EXPECT_EQ((getErrorOfParseIntInBase<2, int>("Ab01")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<8, int>("Ab01")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((getErrorOfParseIntInBase<10, int>("Ab01")), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    EXPECT_EQ((parseIntInBase<16, int>("Ab01")), 43777);
}

/// Asserting `eof` alongside the value is what pins the defect: the old reader returned the correct
/// value 0 for "00" while leaving the second '0' unread, so a value-only assertion passes either way.
template <typename T>
std::pair<T, bool> readUnsafe(std::string_view text)
{
    ReadBufferFromMemory buf(text.data(), text.size());
    T x{};
    readIntTextUnsafe(x, buf);
    return {x, buf.eof()};
}

template <typename T>
int getErrorOfReadUnsafe(std::string_view text)
{
    try
    {
        readUnsafe<T>(text);
        return ErrorCodes::OK;
    }
    catch (Exception & e)
    {
        return e.code();
    }
}

/// The error code alongside the value and `eof`, so a case expecting a value can name the input that
/// threw instead: an escaping exception aborts the whole test body, which loses every later input.
template <typename T>
std::tuple<int, T, bool> readUnsafeOutcome(std::string_view text)
{
    ReadBufferFromMemory buf(text.data(), text.size());
    T x{};
    try
    {
        readIntTextUnsafe(x, buf);
    }
    catch (Exception & e)
    {
        return {e.code(), x, buf.eof()};
    }
    return {ErrorCodes::OK, x, buf.eof()};
}

template <typename T>
std::pair<bool, T> tryReadUnsafe(std::string_view text)
{
    ReadBufferFromMemory buf(text.data(), text.size());
    T x{};
    const bool ok = tryReadIntTextUnsafe(x, buf);
    return {ok, x};
}

/// A value split across two buffers is read whole: `eof` refills, so the run of zeros does not stop
/// at a buffer boundary.
template <typename T>
std::pair<T, bool> readUnsafeSplit(std::string_view head, std::string_view tail)
{
    ReadBufferFromMemory b1(head.data(), head.size());
    ReadBufferFromMemory b2(tail.data(), tail.size());
    ConcatReadBuffer buf(b1, b2);
    T x{};
    readIntTextUnsafe(x, buf);
    return {x, buf.eof()};
}

TEST(ReadIntTextTest, readIntTextUnsafeLeadingZerosAndSigns)
{
    EXPECT_EQ(readUnsafe<Int64>("0"), std::make_pair(Int64(0), true));
    EXPECT_EQ(readUnsafe<Int64>("00"), std::make_pair(Int64(0), true));
    EXPECT_EQ(readUnsafe<Int64>("00000"), std::make_pair(Int64(0), true));
    EXPECT_EQ(readUnsafe<Int64>("007"), std::make_pair(Int64(7), true));
    EXPECT_EQ(readUnsafe<Int64>("03242"), std::make_pair(Int64(3242), true));
    EXPECT_EQ(readUnsafe<Int64>("0100"), std::make_pair(Int64(100), true));
    EXPECT_EQ(readUnsafe<Int64>("000000000000000000007"), std::make_pair(Int64(7), true));

    EXPECT_EQ(readUnsafe<Int64>("-0"), std::make_pair(Int64(0), true));
    EXPECT_EQ(readUnsafe<Int64>("-007"), std::make_pair(Int64(-7), true));
    EXPECT_EQ(readUnsafe<Int64>("-0100"), std::make_pair(Int64(-100), true));

    EXPECT_EQ(readUnsafe<Int64>("+0"), std::make_pair(Int64(0), true));
    EXPECT_EQ(readUnsafe<Int64>("+7"), std::make_pair(Int64(7), true));
    EXPECT_EQ(readUnsafe<Int64>("+007"), std::make_pair(Int64(7), true));

    EXPECT_EQ(readUnsafe<UInt64>("007"), std::make_pair(UInt64(7), true));
    EXPECT_EQ(readUnsafe<UInt64>("+7"), std::make_pair(UInt64(7), true));

    /// A leading zero run stops at the first byte that is not a digit, leaving the rest to the caller.
    EXPECT_EQ(readUnsafe<Int64>("007\t9"), std::make_pair(Int64(7), false));
    EXPECT_EQ(readUnsafe<Int64>("00\t9"), std::make_pair(Int64(0), false));
    EXPECT_EQ(readUnsafe<Int64>("0\t9"), std::make_pair(Int64(0), false));

    EXPECT_EQ(tryReadUnsafe<Int64>("007"), std::make_pair(true, Int64(7)));
    EXPECT_EQ(tryReadUnsafe<Int64>("+007"), std::make_pair(true, Int64(7)));

    EXPECT_EQ((readUnsafeSplit<Int64>("00", "7")), std::make_pair(Int64(7), true));
    EXPECT_EQ((readUnsafeSplit<Int64>("000", "0")), std::make_pair(Int64(0), true));
    EXPECT_EQ((readUnsafeSplit<Int64>("0", "042")), std::make_pair(Int64(42), true));
}

TEST(ReadIntTextTest, readIntTextUnsafeRejectsPlusWithoutDigits)
{
    /// A '+' is newly consumed as a sign, so this pins that the reader refuses one with no digit after
    /// it rather than reading the field as zero. Only the '+' branch carries the requirement; the next
    /// case is why '-' does not.
    for (const auto * text : {"+", "+x", "+-", "++", "+\t1", "+-7", "++7"})
        EXPECT_EQ(getErrorOfReadUnsafe<Int64>(text), ErrorCodes::CANNOT_PARSE_NUMBER) << "input: " << text;

    EXPECT_EQ(tryReadUnsafe<Int64>("+").first, false);
}

TEST(ReadIntTextTest, readIntTextUnsafeLeavesMinusToTheCaller)
{
    /// `TabSeparatedRowInputFormat.cpp:500` documents that a field of just a minus sign reads as zero
    /// for signed types, so the '-' branch requires no digit. Every case asserts the value AND that
    /// something is left unread: the remainder is what the format layer then refuses, so a value-only
    /// assertion would not distinguish "stopped after the sign" from "consumed the whole field". A
    /// second sign is never consumed either, because the sign branches are mutually exclusive -- were
    /// the '+' of "-+7" taken as a sign, the value would be -7 rather than 0 with "+7" left unread.
    for (const auto * text : {"-\t1", "-\n", "-x", "- ", "--", "-+", "-+7", "--7", "-+0", "-+007"})
        EXPECT_EQ(readUnsafeOutcome<Int64>(text), std::make_tuple(ErrorCodes::OK, Int64(0), false))
            << "input: " << text;

    EXPECT_EQ(tryReadUnsafe<Int64>("-\t1"), std::make_pair(true, Int64(0)));
    EXPECT_EQ(tryReadUnsafe<Int64>("-+7"), std::make_pair(true, Int64(0)));

    /// A lone '-' with nothing after it is the one case that throws, and it is the pre-existing
    /// end-of-stream error rather than a digit requirement.
    EXPECT_EQ(getErrorOfReadUnsafe<Int64>("-"), ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
    EXPECT_EQ(tryReadUnsafe<Int64>("-").first, false);

    /// A minus on an unsigned type is not a sign at all, so it is left to the caller as before.
    EXPECT_EQ(readUnsafe<UInt64>("-7"), std::make_pair(UInt64(0), false));
}
