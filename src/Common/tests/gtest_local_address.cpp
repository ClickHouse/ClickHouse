#include <gtest/gtest.h>
#include <Common/isLocalAddress.h>
#include <Common/ShellCommand.h>
#include <Poco/Net/IPAddress.h>
#include <IO/ReadHelpers.h>


TEST(LocalAddress, SmokeTest)
{
    DB::ShellCommand::Config config("/bin/hostname");
    config.arguments = {"-i"};
    auto cmd = DB::ShellCommand::executeDirect(config);

    std::string address_str;
    DB::readString(address_str, cmd->out);
    cmd->wait();
    std::cerr << "Got Address: " << address_str << std::endl;

    /// hostname -i can return more than one address: "2001:db8:1::242:ac11:2 172.17.0.2"
    if (auto space_pos = address_str.find(' '); space_pos != std::string::npos)
        address_str = address_str.substr(0, space_pos);

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
