#include <thread>

#include <Common/ShellCommand.h>
#include <Common/Exception.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>

#include <base/scope_guard.h>

/** This example shows how we can proxy stdin to ShellCommand and obtain stdout in streaming fashion. */

int main(int argc, char ** argv)
try
{
    using namespace DB;

    if (argc < 2)
    {
        std::cerr << "Usage: shell_command_inout 'command...' < in > out\n";
        return 1;
    }

    auto command = ShellCommand::execute(argv[1]);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    WriteBufferFromFileDescriptor err(STDERR_FILENO);
    SCOPE_EXIT(out.finalize(); err.finalize());

    /// Background thread sends data and foreground thread receives result.

    std::thread thread([&]
    {
        copyData(in, command->in);
        command->in.close();
    });

    copyData(command->out, out);
    copyData(command->err, err);

    thread.join();
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
