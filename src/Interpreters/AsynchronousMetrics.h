#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFile.h>

#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <optional>
#include <unordered_map>


namespace Poco
{
class Logger;
}

namespace DB
{

class ProtocolServerAdapter;
class ReadBuffer;

using AsynchronousMetricValue = double;
using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;

struct ProtocolServerMetrics
{
    String port_name;
    size_t current_threads;
};

/** Periodically (by default, each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  *
  * This includes both ClickHouse-related metrics (like memory usage of ClickHouse process)
  *  and common OS-related metrics (like total memory usage on the server).
  */
class AsynchronousMetrics : WithContext
{
public:
    using ProtocolServerMetricsFunc = std::function<std::vector<ProtocolServerMetrics>()>;
    AsynchronousMetrics(
        ContextPtr global_context_,
        int update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_);

    ~AsynchronousMetrics();

    /// Separate method allows to initialize the `servers` variable beforehand.
    void start();

    void stop();

    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

private:
    const std::chrono::seconds update_period;
    ProtocolServerMetricsFunc protocol_server_metrics_func;

    mutable std::mutex mutex;
    std::condition_variable wait_cond;
    bool quit {false};
    AsynchronousMetricValues values;

    /// Some values are incremental and we have to calculate the difference.
    /// On first run we will only collect the values to subtract later.
    bool first_run = true;
    std::chrono::system_clock::time_point previous_update_time;

#if defined(OS_LINUX) || defined(OS_FREEBSD)
    MemoryStatisticsOS memory_stat;
#endif

#if defined(OS_LINUX)
    std::optional<ReadBufferFromFilePRead> meminfo;
    std::optional<ReadBufferFromFilePRead> loadavg;
    std::optional<ReadBufferFromFilePRead> proc_stat;
    std::optional<ReadBufferFromFilePRead> cpuinfo;
    std::optional<ReadBufferFromFilePRead> file_nr;
    std::optional<ReadBufferFromFilePRead> uptime;
    std::optional<ReadBufferFromFilePRead> net_dev;

    std::vector<std::unique_ptr<ReadBufferFromFilePRead>> thermal;

    std::unordered_map<String /* device name */,
        std::unordered_map<String /* label name */,
            std::unique_ptr<ReadBufferFromFilePRead>>> hwmon_devices;

    std::vector<std::pair<
        std::unique_ptr<ReadBufferFromFilePRead> /* correctable errors */,
        std::unique_ptr<ReadBufferFromFilePRead> /* uncorrectable errors */>> edac;

    std::unordered_map<String /* device name */, std::unique_ptr<ReadBufferFromFilePRead>> block_devs;

    /// TODO: socket statistics.

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

    /// https://www.kernel.org/doc/Documentation/block/stat.txt
    struct BlockDeviceStatValues
    {
        uint64_t read_ios;
        uint64_t read_merges;
        uint64_t read_sectors;
        uint64_t read_ticks;
        uint64_t write_ios;
        uint64_t write_merges;
        uint64_t write_sectors;
        uint64_t write_ticks;
        uint64_t in_flight_ios;
        uint64_t io_ticks;
        uint64_t time_in_queue;
        uint64_t discard_ops;
        uint64_t discard_merges;
        uint64_t discard_sectors;
        uint64_t discard_ticks;

        void read(ReadBuffer & in);
        BlockDeviceStatValues operator-(const BlockDeviceStatValues & other) const;
    };

    std::unordered_map<String /* device name */, BlockDeviceStatValues> block_device_stats;

    struct NetworkInterfaceStatValues
    {
        uint64_t recv_bytes;
        uint64_t recv_packets;
        uint64_t recv_errors;
        uint64_t recv_drop;
        uint64_t send_bytes;
        uint64_t send_packets;
        uint64_t send_errors;
        uint64_t send_drop;

        NetworkInterfaceStatValues operator-(const NetworkInterfaceStatValues & other) const;
    };

    std::unordered_map<String /* device name */, NetworkInterfaceStatValues> network_interface_stats;

    Stopwatch block_devices_rescan_delay;

    void openSensors();
    void openBlockDevices();
    void openSensorsChips();
    void openEDAC();
#endif

    std::unique_ptr<ThreadFromGlobalPool> thread;

    void run();
    void update(std::chrono::system_clock::time_point update_time);

    Poco::Logger * log;
};

}
