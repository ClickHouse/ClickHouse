#include <iostream>
#include "src/Core/Types.h"
#include "src/Common/ShellCommand.h"
#include "src/IO/copyData.h"
#include "src/IO/WriteBufferFromFileDescriptor.h"
#include "src/IO/ReadBufferFromString.h"
#include "src/IO/ReadHelpers.h"

#include <chrono>
#include <thread>

#include <gtest/gtest.h>


using namespace DB;


TEST(ShellCommand, Execute)
{
    auto command = ShellCommand::execute("echo 'Hello, world!'");

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, ExecuteDirect)
{
    auto command = ShellCommand::executeDirect("/bin/echo", {"Hello, world!"});

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, ExecuteWithInput)
{
    auto command = ShellCommand::execute("cat");

    String in_str = "Hello, world!\n";
    ReadBufferFromString in(in_str);
    copyData(in, command->in);
    command->in.close();

    std::string res;
    readStringUntilEOF(res, command->out);
    command->wait();

    EXPECT_EQ(res, "Hello, world!\n");
}

TEST(ShellCommand, AutoWait)
{
    // <defunct> hunting:
    for (int i = 0; i < 1000; ++i)
    {
        auto command = ShellCommand::execute("echo " + std::to_string(i));
        //command->wait(); // now automatic
    }

    // std::cerr << "inspect me: ps auxwwf\n";
    // std::this_thread::sleep_for(std::chrono::seconds(100));
}
