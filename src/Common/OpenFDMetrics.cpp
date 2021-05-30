#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "OpenFDMetrics.h"

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

static constexpr auto cmd = "lsof | wc -l";

OpenFDMetrics::OpenFDMetrics() {}

OpenFDMetrics::~OpenFDMetrics() {}

OpenFDMetrics::Data OpenFDMetrics::get() const
{
    Data data;
   
    char buffer[4096];
    String cmd_output = "";
    FILE* pipe = popen(String(cmd).c_str(), "r");
    while (!feof(pipe))
    {
        if (fgets(buffer, 128, pipe))
            cmd_output += buffer;
    }
    pclose(pipe);

    ReadBufferFromString in(cmd_output);

    uint32_t ret;
     
    skipWhitespaceIfAny(in);
    readIntText(ret, in); 

    data.cnt = ret;

    return data;  
}

}
