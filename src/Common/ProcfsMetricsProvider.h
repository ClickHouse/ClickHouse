#pragma once

#include <sys/types.h>
#include <boost/noncopyable.hpp>


#if defined(OS_LINUX)
struct taskstats;

namespace DB
{
/// Provides several essential per-task metrics by reading data from Procfs (when available).
class ProcfsMetricsProvider : private boost::noncopyable
{
public:
    explicit ProcfsMetricsProvider(pid_t /*tid*/);
    ~ProcfsMetricsProvider();

    /// Updates only a part of taskstats struct's fields:
    ///  - cpu_run_virtual_total, cpu_delay_total (when /proc/thread-self/schedstat is available)
    ///  - blkio_delay_total                      (when /proc/thread-self/stat is available)
    ///  - rchar, wchar, read_bytes, write_bytes  (when /proc/thread-self/io is available)
    /// See: man procfs
    void getTaskStats(::taskstats & out_stats) const;

    /// Tells whether this metrics (via Procfs) is provided on the current platform
    static bool isAvailable() noexcept;

private:
    void readParseAndSetThreadCPUStat(::taskstats & out_stats, char *, size_t) const;
    void readParseAndSetThreadBlkIOStat(::taskstats & out_stats, char *, size_t) const;
    void readParseAndSetThreadIOStat(::taskstats & out_stats, char *, size_t) const;

    int thread_schedstat_fd = -1;
    int thread_stat_fd = -1;
    int thread_io_fd = -1;

    /// This field is used for compatibility with TasksStatsCounters::incrementProfileEvents()
    unsigned short stats_version = 1; /// NOLINT
};

}
#endif
