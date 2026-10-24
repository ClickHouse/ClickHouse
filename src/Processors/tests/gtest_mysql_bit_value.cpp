#include "config.h"

#if USE_MYSQL

#include <string>

#include <Processors/Sources/MySQLSource.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

/// A `BIT` value holds at most 64 bits and is transferred in the big-endian order.
///
/// The expected values are numbers rather than byte patterns, so these assertions hold on a host of
/// either endianness. Every width below 8 bytes is covered, because a value shorter than a `UInt64`
/// is where an implementation that writes into the object representation of the result diverges:
/// on a big-endian host such a value ends up left-aligned, and `"\x01\x02"` reads back as
/// `0x0102000000000000`.
TEST(MySQLBitValue, WellFormed)
{
    ASSERT_EQ(DB::parseMySQLBitValue(""), 0);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01", 1)), 0x01);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02", 2)), 0x0102);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02\x03", 3)), 0x010203);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02\x03\x04", 4)), 0x01020304);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02\x03\x04\x05", 5)), 0x0102030405ULL);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02\x03\x04\x05\x06", 6)), 0x010203040506ULL);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02\x03\x04\x05\x06\x07", 7)), 0x01020304050607ULL);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02\x03\x04\x05\x06\x07\x08", 8)), 0x0102030405060708ULL);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8)), 0xFFFFFFFFFFFFFFFFULL);

    /// The high bit of every byte must survive the conversion: a `char` that is signed on most
    /// platforms must not be sign-extended into the accumulator.
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x80", 1)), 0x80);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x00\xFF", 2)), 0x00FF);
}

/// The length of the value comes from the MySQL wire protocol, so an oversized value must be
/// rejected instead of being copied over the destination on the stack.
TEST(MySQLBitValue, Oversized)
{
    ASSERT_THROW(DB::parseMySQLBitValue(std::string(9, '\xFF')), DB::Exception);
    ASSERT_THROW(DB::parseMySQLBitValue(std::string(1024, '\xFF')), DB::Exception);
}

#endif
