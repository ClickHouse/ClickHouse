#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <filesystem>

#include "SchedMetrics.h"

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
}

SchedMetrics::SchedMetrics() {}

SchedMetrics::~SchedMetrics() {}

SchedMetrics::Data SchedMetrics::get() const
{
    DIR* d = opendir("/proc");
    dirent *cur = nullptr;

    std::vector<String> pids;

    while ((cur = readdir(d))) 
    {
       if (cur->d_type == DT_DIR) 
       {
            String name = cur->d_name;
            bool ok = 1;
            for (auto &x : name) 
            {
                ok &= x >= '0' && x <= '9';
            } 
            if (ok) 
                pids.push_back(cur->d_name);   
       }
    }

    Data data;

    for (auto &x : pids) 
    {
        String path = "/proc/" + x + "/status";
        if (!std::filesystem::exists(path))
            continue;
       
        String cmd = "grep ctxt " + path + " | awk '{ print $2 }'";
        char buffer[64];
        String cmd_output = "";
        FILE* pipe = popen(cmd.c_str(), "r");
        while (!feof(pipe))
        {
            if (fgets(buffer, 128, pipe))
                cmd_output += buffer;
        }
        pclose(pipe);
        ReadBufferFromString in(cmd_output);
        try 
        {
            uint32_t ret;
            skipWhitespaceIfAny(in);
            readIntText(ret, in);
            data.total_csw += ret;
            skipWhitespaceIfAny(in);
            readIntText(ret, in);
            data.total_csw += ret;
        } 
        catch(...)
        {
            
        }
    }

    return data;
}

};
