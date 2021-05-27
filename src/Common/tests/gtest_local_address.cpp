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
    std::cerr << "Got Address:" << address_str << std::endl;

    Poco::Net::IPAddress address(address_str);

    EXPECT_TRUE(DB::isLocalAddress(address));
}
