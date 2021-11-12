#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Common/ShellCommand.h>
#include <Common/getMaxFileDescriptorCount.h>

int getMaxFileDescriptorCount()
{
#if defined(__linux__) || defined(__APPLE__)
    using namespace DB;

    auto command = ShellCommand::execute("ulimit -n");
    try
    {
        command->wait();
    }
    catch (...)
    {
        return -1;
    }

    WriteBufferFromOwnString out;
    copyData(command->out, out);

    if (!out.str().empty())
    {
        return parse<Int32>(out.str());
    }

    return -1;
#else
    return -1;
#endif
}
