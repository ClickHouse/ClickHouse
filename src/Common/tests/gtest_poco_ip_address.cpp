#include <gtest/gtest.h>

#include <Poco/Net/IPAddress.h>


TEST(PocoIPAddress, TryParse)
{
    using Family = Poco::Net::IPAddress::Family;
    Poco::Net::IPAddress address;

    ASSERT_TRUE(Poco::Net::IPAddress::tryParse("::", address));
    EXPECT_EQ(address.family(), Family::IPv6);
    EXPECT_TRUE(address.isWildcard());

    ASSERT_TRUE(Poco::Net::IPAddress::tryParse("0.0.0.0", address));
    EXPECT_EQ(address.family(), Family::IPv4);
    EXPECT_TRUE(address.isWildcard());

    ASSERT_TRUE(Poco::Net::IPAddress::tryParse("::1", address));
    EXPECT_TRUE(address.isLoopback());

    /// A string that is not an address is rejected and leaves the result unchanged.
    for (const char * invalid : {"", "not-an-ip", "[::]", ":::"})
    {
        EXPECT_FALSE(Poco::Net::IPAddress::tryParse(invalid, address)) << invalid;
        EXPECT_EQ(address.toString(), "::1") << invalid;
    }
}
