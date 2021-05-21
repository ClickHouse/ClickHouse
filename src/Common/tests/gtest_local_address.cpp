#include <gtest/gtest.h>
#include <Common/isLocalAddress.h>
#include <Common/ShellCommand.h>
#include <Poco/Net/IPAddress.h>
#include <IO/ReadHelpers.h>


TEST(LocalAddress, SmokeTest)
{
    auto cmd = DB::ShellCommand::executeDirect("/bin/hostname", {"-i"});
    std::string address_str;
    DB::readString(address_str, cmd->out);
    cmd->wait();
    std::cerr << "Got Address: " << address_str << std::endl;

    Poco::Net::IPAddress address(address_str);

    EXPECT_TRUE(DB::isLocalAddress(address));
}

TEST(LocalAddress, Localhost)
{
    EXPECT_TRUE(DB::isLocalAddress(Poco::Net::IPAddress{"127.0.0.1"}));
    EXPECT_TRUE(DB::isLocalAddress(Poco::Net::IPAddress{"127.0.1.1"}));
    EXPECT_TRUE(DB::isLocalAddress(Poco::Net::IPAddress{"127.1.1.1"}));
    EXPECT_TRUE(DB::isLocalAddress(Poco::Net::IPAddress{"127.1.0.1"}));
    EXPECT_TRUE(DB::isLocalAddress(Poco::Net::IPAddress{"127.1.0.0"}));
    EXPECT_TRUE(DB::isLocalAddress(Poco::Net::IPAddress{"::1"}));

    /// Make sure we don't mess with the byte order.
    EXPECT_FALSE(DB::isLocalAddress(Poco::Net::IPAddress{"1.0.0.127"}));
    EXPECT_FALSE(DB::isLocalAddress(Poco::Net::IPAddress{"1.1.1.127"}));

    EXPECT_FALSE(DB::isLocalAddress(Poco::Net::IPAddress{"0.0.0.0"}));
    EXPECT_FALSE(DB::isLocalAddress(Poco::Net::IPAddress{"::"}));
    EXPECT_FALSE(DB::isLocalAddress(Poco::Net::IPAddress{"::2"}));

    /// See the comment in the implementation of isLocalAddress.
    EXPECT_FALSE(DB::isLocalAddress(Poco::Net::IPAddress{"127.0.0.2"}));
}
