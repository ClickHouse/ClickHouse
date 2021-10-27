#include <Common/getMaxFileDescriptorCount.h>
#include <Common/ShellCommand.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <string>

int getMaxFileDescriptorCount()
{
#if defined(__linux__)  || defined(__APPLE__)
    using namespace DB;

    auto command = ShellCommand::execute("ulimit -n");

    WriteBufferFromOwnString out;
    copyData(command->out, out);

    if(!out.str().empty())
    {
        return std::stoi(out.str());
    }

    return -1;
#else
    return -1;
#endif

}
