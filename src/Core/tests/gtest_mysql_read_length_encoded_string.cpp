#include <gtest/gtest.h>

#include <Core/MySQL/IMySQLReadPacket.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>

using namespace DB;
using namespace DB::MySQLProtocol;

namespace
{

/// A length-encoded string with a value < 0xfb encodes its length in a single byte.
TEST(MySQLReadLengthEncodedString, ReadsInRangeString)
{
    String wire("\x03""abc", 4);
    ReadBufferFromString buffer(wire);
    String s;
    readLengthEncodedString(s, buffer);
    EXPECT_EQ(s, "abc");
}

/// The 0xfe prefix carries an 8-byte length. A malicious value far larger than the data that
/// actually follows must not be allocated up front: the read fails cleanly instead of trying
/// to resize the string to gigabytes (pre-auth DoS on the MySQL handshake path).
TEST(MySQLReadLengthEncodedString, RejectsOversizedLengthWithoutHugeAllocation)
{
    /// 0xfe + little-endian 0x7000000000000000, with no body bytes following.
    String wire("\xfe\x00\x00\x00\x00\x00\x00\x00\x70", 9);
    ReadBufferFromString buffer(wire);
    String s;
    try
    {
        readLengthEncodedString(s, buffer);
        FAIL() << "expected an exception for an oversized length-encoded string";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("length-encoded string"), String::npos) << e.message();
    }
}

/// A length that overruns the available data is reported after reading what is present,
/// without pre-allocating the full claimed length.
TEST(MySQLReadLengthEncodedString, RejectsLengthLongerThanData)
{
    String wire("\x0a""abc", 4); /// claims 10 bytes, only 3 are present
    ReadBufferFromString buffer(wire);
    String s;
    try
    {
        readLengthEncodedString(s, buffer);
        FAIL() << "expected an exception when the data is shorter than the encoded length";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("length-encoded string"), String::npos) << e.message();
    }
}

}
