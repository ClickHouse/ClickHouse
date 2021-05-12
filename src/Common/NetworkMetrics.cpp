#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

#include "NetworkMetrics.h"

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
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

static constexpr auto filename_traffic = "/proc/net/dev";
static constexpr auto command_traffic = "netstat -s";
static constexpr auto command_connections = "ss -s";

NetworkMetrics::NetworkMetrics()
{
    fd = ::open(filename_traffic, O_RDONLY | O_CLOEXEC);
    if (-1 == fd)
        throwFromErrno("Cannot open file " + std::string(filename_traffic), errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

NetworkMetrics::~NetworkMetrics()
{
    if (0 != ::close(fd))
    {
        try
        {
            throwFromErrno(
                "File descriptor for \"" + std::string(filename_traffic) + "\" could not be closed. "
                "Something seems to have gone wrong. Inspect errno.", ErrorCodes::CANNOT_CLOSE_FILE);
        }
        catch (const ErrnoException &)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

NetworkMetrics::Data NetworkMetrics::get_traffic() const
{
    Data data;
            
    constexpr size_t buf_size = 4096;
    char buf[buf_size];
     
    ssize_t res = 0;
    
    do
    {
        res = ::pread(fd, buf, buf_size, 0);

        if (-1 == res)
        {
            if (errno == EINTR)
                continue;

            throwFromErrno("Cannot read from file " + std::string(filename_traffic), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        assert(res >= 0);
        break;
    }
    while (true);

    ReadBufferFromMemory in(buf, res);

    String unused;
    for (int i = 0; i < 10; i++)  
    {
        readString(unused, in);
        if (unused == "")
            break;
    }
    skipWhitespaceIfAny(in);
    readString(unused, in);
    uint64_t ret;

    while (true)
    {
        skipWhitespaceIfAny(in);
        readStringUntilWhitespace(unused, in);
        if (unused == "") 
            break;
        skipWhitespaceIfAny(in);
        readIntText(ret, in);
        data.received_bytes += ret;
        skipWhitespaceIfAny(in);
        readIntText(ret, in);
        data.received_packets += ret;
        
        for (int i = 0; i < 7; i++) 
        {
            skipWhitespaceIfAny(in);
            readIntText(ret, in);
        }

        data.transmitted_bytes += ret;
        skipWhitespaceIfAny(in);
        readIntText(ret, in);
        data.transmitted_packets += ret;

        readString(unused, in);
    }

    ///Get info about retransmitted packages from netstat -s
    
    char buffer[4096];
    String cmd_output = "";
    FILE* pipe = popen(String(command_traffic).c_str(), "r");
    while (!feof(pipe))
    {
        if (fgets(buffer, 128, pipe))
            cmd_output += buffer;
    }
    pclose(pipe);

    ReadBufferFromString in1(cmd_output);

    for (int i = 0; i < 100; i++)
    {
        readString(unused, in1);
        if (unused.find("Tcp:") != String::npos)
            break;
        skipWhitespaceIfAny(in1);
    }

    for (int i = 0; i < 7; i++) 
    {
        skipWhitespaceIfAny(in1);
        readString(unused, in1);
    }

    skipWhitespaceIfAny(in1);
    readIntText(ret, in1);
 
    data.tcp_retransmit = ret;

    return data;
}

NetworkMetrics::Data NetworkMetrics::get_connections() const
{
    ///Get all info from ss -s

    Data data;

    char buffer[4096];
    String cmd_output = "";
    FILE* pipe = popen(String(command_connections).c_str(), "r");
    while (!feof(pipe))
    {
        if (fgets(buffer, 128, pipe))
            cmd_output += buffer;
    }
    pclose(pipe);

    ReadBufferFromString in(cmd_output);

    String unused;
    readString(unused, in);
    readString(unused, in);
    readString(unused, in);
    readString(unused, in);

    uint64_t ret = 0;
    for (int i = 0; i < 7; i++) 
    {
        skipWhitespaceIfAny(in);
        readStringUntilWhitespace(unused, in);
        skipWhitespaceIfAny(in);
        readIntText(ret, in);
        if (i > 1) 
            data.distinct_hosts += ret;
        if (i == 3)
            data.udp += ret;
        if (i == 4)
            data.tcp += ret;
        readString(unused, in);
    }
    
    return data;
}

NetworkMetrics::Data NetworkMetrics::get() const
{
    Data result;

    Data tmp = get_traffic();
    result.received_bytes = tmp.received_bytes;
    result.received_packets = tmp.received_packets;
    result.transmitted_bytes = tmp.transmitted_bytes;
    result.transmitted_packets = tmp.transmitted_packets;
    result.tcp_retransmit = tmp.tcp_retransmit;

    tmp = get_connections();
    result.distinct_hosts = tmp.distinct_hosts;
    result.tcp = tmp.tcp;
    result.udp = tmp.udp;

    return result;
}

}

