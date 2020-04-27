#pragma once

#include <sys/types.h>
#include <boost/noncopyable.hpp>


struct taskstats;

namespace DB
{
/// Provides several essential per-task metrics by reading data from Procfs (when available).
class ProcfsMetricsProvider : private boost::noncopyable
{
public:
    /// TODO: Do we want to use the supplied thread_id for fetching metrics for arbitrary pids/threads?
    ProcfsMetricsProvider(const pid_t /*tid*/);
    ~ProcfsMetricsProvider();

    /// Updates only a part of taskstats struct's fields:
    ///  - cpu_run_virtual_total, cpu_delay_total (when /proc/[tid]/schedstat is available)
    ///  - blkio_delay_total                      (when /proc/[tid]/stat is available)
    ///  - rchar, wchar, read_bytes, write_bytes  (when /prod/[tid]/io is available)
    void getTaskStats(::taskstats & out_stats) const;

    /// Tells whether this metrics (via Procfs) is provided on the current platform
    static bool isAvailable();

private:
    void readParseAndSetThreadCPUStat(::taskstats & out_stats, char *, size_t) const;
    void readParseAndSetThreadBlkIOStat(::taskstats & out_stats, char *, size_t) const;
    void readParseAndSetThreadIOStat(::taskstats & out_stats, char *, size_t) const;

private:
    int thread_schedstat_fd = -1;
    int thread_stat_fd = -1;
    int thread_io_fd = -1;
    int stats_version = 1;
};

}
