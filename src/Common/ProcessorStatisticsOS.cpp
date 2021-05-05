#if defined(OS_LINUX)

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <string>

#include "ProcessorStatisticsOS.h"


#include <Core/Types.h>

#include <common/logger_useful.h>

#include <Common/Exception.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/MMappedFileDescriptor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_CLOSE_FILE;
}

static constexpr auto loadavg_filename = "/proc/loadavg";
static constexpr auto procst_filename  = "/proc/stat";
static constexpr auto cpuinfo_filename = "/proc/cpuinfo";

ProcessorStatisticsOS::ProcessorStatisticsOS()
    : loadavg_fd(openWithCheck(loadavg_filename, O_RDONLY | O_CLOEXEC))
    , procst_fd(openWithCheck(procst_filename,   O_RDONLY | O_CLOEXEC))
    , cpuinfo_fd(openWithCheck(cpuinfo_filename, O_RDONLY | O_CLOEXEC))
{}

ProcessorStatisticsOS::~ProcessorStatisticsOS() 
{
    closeFD(loadavg_fd, String(loadavg_filename));
    closeFD(procst_fd, String(procst_filename));
    closeFD(cpuinfo_fd, String(cpuinfo_filename));
}

int ProcessorStatisticsOS::openWithCheck(const String & filename, int flags)
{
    int fd = ::open(filename.c_str(), flags);
    checkFDAfterOpen(fd, filename);
    return fd;
}

void ProcessorStatisticsOS::checkFDAfterOpen(int fd, const String & filename)
{
    if (-1 == fd) 
        throwFromErrno(
                "Cannot open file" + String(filename), 
                errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}

void ProcessorStatisticsOS::closeFD(int fd, const String & filename)
{
    if (0 != ::close(fd)) 
    {
        try 
        {
            throwFromErrno(
                    "File descriptor for \"" + filename + "\" could not be closed. "
                    "Something seems to have gone wrong. Inspect errno.", ErrorCodes::CANNOT_CLOSE_FILE);
        } catch(const ErrnoException&)
        {
            DB::tryLogCurrentException(__PRETTY_FUNCTION__); 
        }
    }
}

ProcessorStatisticsOS::Data ProcessorStatisticsOS::ProcessorStatisticsOS::get() const 
{
    Data data;
    readLoadavg(data);
    readProcst(data);
    readCpuinfo(data);
    return data;
}

void ProcessorStatisticsOS::readLoadavg(Data & data) const
{
    constexpr size_t buf_size = 1024;
    char buf[buf_size];

    ssize_t res = 0;

    do 
    {
        res = ::pread(loadavg_fd, buf, buf_size, 0);
        
        if (-1 == res) 
        {
            if (errno == EINTR) 
                continue;

            throwFromErrno("Cannot read from file " + String(loadavg_filename), 
                    ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        assert(res >= 0);
        break;
    } while (true);

    ReadBufferFromMemory in(buf, res);
    
    readFloatAndSkipWhitespaceIfAny(data.avg1,  in);
    readFloatAndSkipWhitespaceIfAny(data.avg5,  in);
    readFloatAndSkipWhitespaceIfAny(data.avg15, in);
}

void ProcessorStatisticsOS::readProcst(Data & data) const
{
    MMappedFileDescriptor mapped_procst(procst_fd, 0);
    ReadBufferFromMemory in(mapped_procst.getData(),
                            mapped_procst.getLength());

    String field_name, field_val;
    uint64_t unused; 
   
    readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_name, in);

    readIntTextAndSkipWhitespaceIfAny(data.user_time,   in);
    readIntTextAndSkipWhitespaceIfAny(data.nice_time,   in);
    readIntTextAndSkipWhitespaceIfAny(data.system_time, in);
    readIntTextAndSkipWhitespaceIfAny(data.idle_time,   in);
    readIntTextAndSkipWhitespaceIfAny(data.iowait_time, in);
    
    readIntTextAndSkipWhitespaceIfAny(unused, in);
    readIntTextAndSkipWhitespaceIfAny(unused, in);
    
    readIntTextAndSkipWhitespaceIfAny(data.steal_time,  in);
    readIntTextAndSkipWhitespaceIfAny(data.guest_time,  in);
    readIntTextAndSkipWhitespaceIfAny(data.nice_time,   in);

    do 
    {
        readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_name, in);
        readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_val,  in);
    } while (field_name != String("processes"));
    
    data.processes = static_cast<uint32_t>(std::stoul(field_val));
    
    readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_name, in);
    readIntTextAndSkipWhitespaceIfAny(data.procs_running, in);
    
    readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_name, in);
    readIntTextAndSkipWhitespaceIfAny(data.procs_blocked, in);
}

void ProcessorStatisticsOS::readCpuinfo(Data & data) const
{
    MMappedFileDescriptor mapped_cpuinfo(cpuinfo_fd, 0);
    ReadBufferFromMemory in(mapped_cpuinfo.getData(), 
                            mapped_cpuinfo.getLength());
    
    String field_name, field_val;
    char unused;

    do 
    {
        
        readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_name, in);
        readCharAndSkipWhitespaceIfAny(unused,        in);
        readStringUntilWhitespaceAndSkipWhitespaceIfAny(field_val,  in);
    } while (field_name != String("cpu MHz"));
    
    data.freq = stof(field_val);
}

template<typename T>
void ProcessorStatisticsOS::readIntTextAndSkipWhitespaceIfAny(T& x, ReadBuffer& buf)
{
    readIntText(x, buf);
    skipWhitespaceIfAny(buf);
}

void ProcessorStatisticsOS::readStringUntilWhitespaceAndSkipWhitespaceIfAny(String & s, ReadBuffer & buf)
{
    readStringUntilWhitespace(s, buf);
    skipWhitespaceIfAny(buf);
}

void ProcessorStatisticsOS::readCharAndSkipWhitespaceIfAny(char & c, ReadBuffer & buf)
{
    readChar(c, buf);
    skipWhitespaceIfAny(buf);
}

void ProcessorStatisticsOS::readFloatAndSkipWhitespaceIfAny(float & f, ReadBuffer & buf)
{
    readFloatText(f, buf);
    skipWhitespaceIfAny(buf);
}

}

#endif
