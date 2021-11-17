#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Common/ShellCommand.h>
#include <Common/getMaxFileDescriptorCount.h>

int getMaxFileDescriptorCount()
{
    int result = -1;
#if defined(__linux__) || defined(__APPLE__)
    using namespace DB;

    auto command = ShellCommand::execute("ulimit -n");
    try
    {
        readIntText(result, command->out);
        command->wait();
    }
    catch (...)
    {
    }

#endif

    return result;
}
