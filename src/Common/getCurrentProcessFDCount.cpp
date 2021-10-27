#include <Common/getCurrentProcessFDCount.h>
#include <Common/ShellCommand.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <string>
#include <cstdio>
#include <unistd.h>


int getCurrentProcessFDCount()
{
#if defined(__linux__)  || defined(__APPLE__)
    using namespace DB;

    char buf[64];
    snprintf(buf, 64, "lsof -p %i | wc -l", getpid());

    auto command = ShellCommand::execute(buf);

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
