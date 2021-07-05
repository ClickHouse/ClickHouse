#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBufferFromFile.h>

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <optional>
#include <unordered_map>


namespace DB
{

class ProtocolServerAdapter;
class ReadBuffer;

using AsynchronousMetricValue = double;
using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;


/** Periodically (by default, each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  */
class AsynchronousMetrics : WithContext
{
public:
    /// The default value of update_period_seconds is for ClickHouse-over-YT
    /// in Arcadia -- it uses its own server implementation that also uses these
    /// metrics.
    AsynchronousMetrics(
        ContextPtr global_context_,
        int update_period_seconds,
        std::shared_ptr<std::vector<ProtocolServerAdapter>> servers_to_start_before_tables_,
        std::shared_ptr<std::vector<ProtocolServerAdapter>> servers_);

    ~AsynchronousMetrics();

    /// Separate method allows to initialize the `servers` variable beforehand.
    void start();

    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

#if defined(ARCADIA_BUILD)
    /// This constructor needs only to provide backward compatibility with some other projects (hello, Arcadia).
    /// Never use this in the ClickHouse codebase.
    AsynchronousMetrics(
        ContextPtr global_context_,
        int update_period_seconds = 60)
        : WithContext(global_context_)
        , update_period(update_period_seconds)
    {
    }
#endif

private:
    const std::chrono::seconds update_period;
    std::shared_ptr<std::vector<ProtocolServerAdapter>> servers_to_start_before_tables{nullptr};
    std::shared_ptr<std::vector<ProtocolServerAdapter>> servers{nullptr};

    mutable std::mutex mutex;
    std::condition_variable wait_cond;
    bool quit {false};
    AsynchronousMetricValues values;

    /// Some values are incremental and we have to calculate the difference.
    /// On first run we will only collect the values to subtract later.
    bool first_run = true;
    std::chrono::system_clock::time_point previous_update_time;

#if defined(OS_LINUX)
    MemoryStatisticsOS memory_stat;

    std::optional<ReadBufferFromFile> meminfo;
    std::optional<ReadBufferFromFile> loadavg;
    std::optional<ReadBufferFromFile> proc_stat;
    std::optional<ReadBufferFromFile> cpuinfo;
    std::optional<ReadBufferFromFile> file_nr;
    std::optional<ReadBufferFromFile> uptime;
    std::vector<std::unique_ptr<ReadBufferFromFile>> thermal;

    std::unordered_map<String /* device name */,
        std::unordered_map<String /* label name */,
            std::unique_ptr<ReadBufferFromFile>>> hwmon_devices;

    /// TODO: IO load, Network rx/tx, sockets, EDAC.

    struct ProcStatValuesCPU
    {
        uint64_t user;
        uint64_t nice;
        uint64_t system;
        uint64_t idle;
        uint64_t iowait;
        uint64_t irq;
        uint64_t softirq;
        uint64_t steal;
        uint64_t guest;
        uint64_t guest_nice;

        void read(ReadBuffer & in);
        ProcStatValuesCPU operator-(const ProcStatValuesCPU & other) const;
    };

    struct ProcStatValuesOther
    {
        uint64_t interrupts;
        uint64_t context_switches;
        uint64_t processes_created;

        ProcStatValuesOther operator-(const ProcStatValuesOther & other) const;
    };

    ProcStatValuesCPU proc_stat_values_all_cpus{};
    ProcStatValuesOther proc_stat_values_other{};
    std::vector<ProcStatValuesCPU> proc_stat_values_per_cpu;

#endif

    std::unique_ptr<ThreadFromGlobalPool> thread;

    void run();
    void update(std::chrono::system_clock::time_point update_time);
};

}
