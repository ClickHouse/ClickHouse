#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#include "IOMetrics.h"

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

static constexpr auto command_traffic = "iostat -d";
static constexpr auto command_util = "iostat -xd";

IOMetrics::IOMetrics() {}

IOMetrics::~IOMetrics() {}

IOMetrics::Data IOMetrics::get() const
{
    Data data;
    
    char buffer[4096];
    String cmd_output = "";
    FILE* pipe = popen(String(command_traffic).c_str(), "r");
    while (!feof(pipe))
    {
        if (fgets(buffer, 128, pipe))
            cmd_output += buffer;
    }
    pclose(pipe);

    ReadBufferFromString in(cmd_output);
    String unused;
    double ret;
    int cnt = 0;
    while (true) {
        readString(unused, in);
        if (unused.find("kB_dscd") != String::npos) 
           break;
       skipWhitespaceIfAny(in); 
    }
    while (true)
    {
        readStringUntilWhitespace(unused, in);        
        if (unused == "")
            break;
        String device = unused;
        while (device.back() == ' ' || device.back() == '\t') 
            device.pop_back();
        skipWhitespaceIfAny(in);
        try
        {
            readFloatText(ret, in);
        }
        catch(...)
        {
            break;
        }
        data.tps_total += ret;
        data.dev_tps.push_back({device, ret});
        skipWhitespaceIfAny(in);
        readFloatText(ret, in);
        data.read_total += ret;
        data.dev_read.push_back({device, ret});
        skipWhitespaceIfAny(in);
        readFloatText(ret, in);
        data.write_total += ret;
        data.dev_write.push_back({device, ret});
        readString(unused, in); 
        cnt++;
    }
    data.tps_avg = data.tps_total / cnt;
    data.read_avg = data.read_total / cnt;
    data.write_avg = data.write_total / cnt;
    cmd_output = "";
    pipe = popen(String(command_util).c_str(), "r");
    while (!feof(pipe))
    {
        if (fgets(buffer, 128, pipe))
            cmd_output += buffer;
    }
    pclose(pipe);

    ReadBufferFromString in1(cmd_output);  
    while (true) {
        readString(unused, in1);
        if (unused.find("util") != String::npos) 
           break;
       skipWhitespaceIfAny(in1); 
    }
    cnt = 0;
    while (true) 
    {
        readStringUntilWhitespace(unused, in1);
        if (unused == "")
            break;
        String device = unused;
        skipWhitespaceIfAny(in1);
        while (device.back() == ' ' || device.back() == '\t') 
            device.pop_back();
        for (int i = 0; i < 18; i++) 
        {
            try
            {
                readFloatText(ret, in1);
            }
            catch(...)
            {
                data.queue_size_avg = data.queue_size_total / cnt;
                data.util_avg = data.util_total / cnt;
                return data; 
            }
            skipWhitespaceIfAny(in1);
        }
        readFloatText(ret, in1);
        data.queue_size_total += ret;
        data.dev_queue_size.push_back({device, ret});
        skipWhitespaceIfAny(in1);
        readFloatText(ret, in1);
        data.util_total += ret;
        data.dev_util.push_back({device, ret});
        cnt++;
    }

    data.queue_size_avg = data.queue_size_total / cnt;
    data.util_avg = data.util_total / cnt;
    return data;
}

}
