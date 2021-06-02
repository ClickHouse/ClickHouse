#pragma once
#if defined(OS_LINUX)

#include <cstdint>
#include <string>

#include <Core/Types.h>

#include <IO/ReadBufferFromFile.h>

namespace DB
{

/** Opens files: /proc/loadavg, /proc/stat, /proc/cpuinfo and reads processor statistics in get() method.
  * This is Linux specific.
  * See: man procfs
  */
class ProcessorStatisticsOS
{
public:
    struct ProcLoadavg
    {
        float avg1;
        float avg5;
        float avg15;
    };

    struct ProcStLoad
    {
        float user_time;
        float nice_time;
        float system_time;
        float idle_time;
        float iowait_time;
        float steal_time;
        float guest_time;
        float guest_nice_time;

        uint32_t processes;
        uint32_t procs_running;
        uint32_t procs_blocked;
    };

    struct ProcFreq
    {
        float max;
        float min;
        float avg;
    };

    struct Data
    {
        ProcLoadavg loadavg;
        ProcStLoad stload;
        ProcFreq freq;
    };

    ProcessorStatisticsOS();
    ~ProcessorStatisticsOS();

    Data get();

private:
    struct ProcTime
    {
        // The amount of time, measured in seconds
        uint64_t user;
        uint64_t nice;
        uint64_t system;
        uint64_t idle;
        uint64_t iowait;
        uint64_t steal;
        uint64_t guest;
        uint64_t guest_nice;
    };

    void readLoadavg(ProcLoadavg & loadavg);
    void calcStLoad(ProcStLoad & stload);
    void readFreq(ProcFreq & freq);

    void readProcTimeAndProcesses(ProcTime & proc_time, ProcStLoad & stload);

private:
    std::time_t last_stload_call_time;
    ProcTime last_proc_time;
};

}

#endif
