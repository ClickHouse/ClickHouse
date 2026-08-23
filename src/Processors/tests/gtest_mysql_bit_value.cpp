#include "config.h"

#if USE_MYSQL

#include <string>

#include <Processors/Sources/MySQLSource.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

/// A `BIT` value holds at most 64 bits and is transferred in the big-endian order.
TEST(MySQLBitValue, WellFormed)
{
    ASSERT_EQ(DB::parseMySQLBitValue(""), 0);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01", 1)), 1);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\x01\x02", 2)), 0x0102);
    ASSERT_EQ(DB::parseMySQLBitValue(std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8)), 0xFFFFFFFFFFFFFFFFULL);
}

/// The length of the value comes from the MySQL wire protocol, so an oversized value must be
/// rejected instead of being copied over the destination on the stack.
TEST(MySQLBitValue, Oversized)
{
    ASSERT_THROW(DB::parseMySQLBitValue(std::string(9, '\xFF')), DB::Exception);
    ASSERT_THROW(DB::parseMySQLBitValue(std::string(1024, '\xFF')), DB::Exception);
}

#endif
