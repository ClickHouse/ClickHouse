#include <iostream>
#include <Core/Types.h>
#include <Common/ShellCommand.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromString.h>

#include <chrono>
#include <thread>

using namespace DB;


int main(int, char **)
try
{
    {
        auto command = ShellCommand::execute("echo 'Hello, world!'");

        WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        copyData(command->out, out);

        command->wait();
    }

    {
        auto command = ShellCommand::executeDirect("/bin/echo", {"Hello, world!"});

        WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        copyData(command->out, out);

        command->wait();
    }

    {
        auto command = ShellCommand::execute("cat");

        String in_str = "Hello, world!\n";
        ReadBufferFromString in(in_str);
        copyData(in, command->in);
        command->in.close();

        WriteBufferFromFileDescriptor out(STDOUT_FILENO);
        copyData(command->out, out);

        command->wait();
    }

    // <defunct> hunting:
    for (int i = 0; i < 1000; ++i)
    {
        auto command = ShellCommand::execute("echo " + std::to_string(i));
        //command->wait(); // now automatic
    }

    // std::cerr << "inspect me: ps auxwwf" << "\n";
    // std::this_thread::sleep_for(std::chrono::seconds(100));
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(false) << "\n";
    return 1;
}
