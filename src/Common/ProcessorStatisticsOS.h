#pragma once
#if defined(OS_LINUX)

#include <cstdint>
#include <string>

#include <Core/Types.h>

#include <IO/ReadBuffer.h>

namespace DB 
{

/** Opens files: /proc/loadav, /proc/stat, /proc/cpuinfo. Keeps it open and reads processor statistics.
  * This is Linux specific.
  * See: man procfs
  */
   
class ProcessorStatisticsOS 
{
public:
    struct Data
    {
        float avg1;
        float avg5;
        float avg15;

        /** The amount of time, measured in units of USER_HZ 
          * (1/100ths of a second on most architectures, use sysconf(_SC_CLK_TCK) to obtain the right value)
          */
        uint64_t user_time;
        uint64_t nice_time;
        uint64_t system_time;
        uint64_t idle_time;
        uint64_t iowait_time;
        uint64_t steal_time;
        uint64_t guest_time;
        uint64_t guest_nice_time;

        uint32_t processes;
        uint32_t procs_running;
        uint32_t procs_blocked;

        float freq;
    };

    ProcessorStatisticsOS();
    ~ProcessorStatisticsOS();
    
    Data get() const;

private:
    static int  openWithCheck(const String & filename, int flags);
    
    static void checkFDAfterOpen(int fd, const String & filename);

    static void closeFD(int fd, const String & filename);

    template<typename T>
    static void readIntTextAndSkipWhitespaceIfAny(T & x, ReadBuffer & buf); 

    static void readStringUntilWhitespaceAndSkipWhitespaceIfAny(String & s, ReadBuffer & buf);

    static void readCharAndSkipWhitespaceIfAny(char & c, ReadBuffer & buf);

    static void readFloatAndSkipWhitespaceIfAny(float & f, ReadBuffer & buf);

    void readLoadavg(Data & data) const;
    void readProcst(Data  & data) const;
    void readCpuinfo(Data & data) const;

private:
    int loadavg_fd;
    int procst_fd;
    int cpuinfo_fd;
};

}

#endif
