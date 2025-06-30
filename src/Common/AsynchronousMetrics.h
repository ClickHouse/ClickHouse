#pragma once

#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Common/SharedMutex.h>
#include <IO/ReadBufferFromFile.h>

#include <condition_variable>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>


namespace Poco
{
    class Logger;
}

namespace DB
{

class ReadBuffer;

struct AsynchronousMetricValue
{
    double value;
    const char * documentation;

    template <typename T>
    AsynchronousMetricValue(T value_, const char * documentation_)
        : value(static_cast<double>(value_)), documentation(documentation_) {}
    AsynchronousMetricValue() = default; /// For std::unordered_map::operator[].
};

using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;

struct ProtocolServerMetrics
{
    String port_name;
    size_t current_threads;
    size_t rejected_connections;
};

/** Periodically (by default, each second)
  * calculates and updates some metrics,
  * that are not updated automatically (so, need to be asynchronously calculated).
  *
  * This includes both general process metrics (like memory usage)
  * and common OS-related metrics (like total memory usage on the server).
  *
  * All the values are either gauge type (like the total number of tables, the current memory usage).
  * Or delta-counters representing some accumulation during the interval of time.
  *
  * Server and Keeper specific metrics are contained inside
  * ServerAsynchronousMetrics and KeeperAsynchronousMetrics respectively.
  */
class AsynchronousMetrics
{
protected:
    using Duration = std::chrono::seconds;
    using TimePoint = std::chrono::system_clock::time_point;

public:
    using ProtocolServerMetricsFunc = std::function<std::vector<ProtocolServerMetrics>()>;

    AsynchronousMetrics(
        unsigned update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
        bool update_jemalloc_epoch_,
        bool update_rss_,
        const ContextPtr & context_);

    virtual ~AsynchronousMetrics();

    /// Separate method allows to initialize the `servers` variable beforehand.
    void start();

    void stop();

    void update(TimePoint update_time, bool force_update = false);

    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

protected:
    const Duration update_period;

    LoggerPtr log;
private:
    virtual void updateImpl(TimePoint update_time, TimePoint current_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values) = 0;
    virtual void logImpl(AsynchronousMetricValues &) { }
    static auto tryGetMetricValue(const AsynchronousMetricValues & values, const String & metric, size_t default_value = 0);
    void processWarningForMutationStats(const AsynchronousMetricValues & new_values) const;

    ProtocolServerMetricsFunc protocol_server_metrics_func;

    std::unique_ptr<ThreadFromGlobalPool> thread;

    mutable std::mutex thread_mutex;
    std::condition_variable wait_cond;
    bool quit TSA_GUARDED_BY(thread_mutex) = false;

    /// Protects all raw data and serializes multiple updates.
    mutable std::mutex data_mutex;

    /// Some values are incremental and we have to calculate the difference.
    /// On first run we will only collect the values to subtract later.
    bool first_run TSA_GUARDED_BY(data_mutex) = true;
    TimePoint previous_update_time TSA_GUARDED_BY(data_mutex);

    /// Protects saved values.
    mutable SharedMutex values_mutex;
    /// Values store the result of the last update prepared for reading.
    AsynchronousMetricValues values TSA_GUARDED_BY(values_mutex);

#if defined(OS_LINUX) || defined(OS_FREEBSD)
    MemoryStatisticsOS memory_stat TSA_GUARDED_BY(data_mutex);
#endif

    [[maybe_unused]] const bool update_jemalloc_epoch;
    [[maybe_unused]] const bool update_rss;
    ContextPtr context;

#if defined(OS_LINUX)
    std::optional<ReadBufferFromFilePRead> meminfo TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> loadavg TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> proc_stat TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cpuinfo TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> file_nr TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> uptime TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> net_dev TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> net_tcp TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> net_tcp6 TSA_GUARDED_BY(data_mutex);

    std::optional<ReadBufferFromFilePRead> cpu_pressure TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> memory_pressure TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> io_pressure TSA_GUARDED_BY(data_mutex);

    std::unordered_map<String /* PSI stall type */, uint64_t> prev_pressure_vals TSA_GUARDED_BY(data_mutex);

    std::optional<ReadBufferFromFilePRead> cgroupmem_limit_in_bytes TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cgroupmem_usage_in_bytes TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cgroupcpu_cfs_period TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cgroupcpu_cfs_quota TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cgroupcpu_max TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cgroupcpu_stat TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> cgroupcpuacct_stat TSA_GUARDED_BY(data_mutex);

    std::optional<ReadBufferFromFilePRead> vm_max_map_count TSA_GUARDED_BY(data_mutex);
    std::optional<ReadBufferFromFilePRead> vm_maps TSA_GUARDED_BY(data_mutex);

    std::vector<std::unique_ptr<ReadBufferFromFilePRead>> thermal TSA_GUARDED_BY(data_mutex);

    std::unordered_map<String /* device name */,
        std::unordered_map<String /* label name */,
            std::unique_ptr<ReadBufferFromFilePRead>>> hwmon_devices TSA_GUARDED_BY(data_mutex);

    std::vector<std::pair<
        std::unique_ptr<ReadBufferFromFilePRead> /* correctable errors */,
        std::unique_ptr<ReadBufferFromFilePRead> /* uncorrectable errors */>> edac TSA_GUARDED_BY(data_mutex);

    std::unordered_map<String /* device name */, std::unique_ptr<ReadBufferFromFilePRead>> block_devs TSA_GUARDED_BY(data_mutex);

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

    ProcStatValuesCPU proc_stat_values_all_cpus TSA_GUARDED_BY(data_mutex) {};
    ProcStatValuesOther proc_stat_values_other TSA_GUARDED_BY(data_mutex) {};
    std::vector<ProcStatValuesCPU> proc_stat_values_per_cpu TSA_GUARDED_BY(data_mutex);

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

    std::unordered_map<String /* device name */, BlockDeviceStatValues> block_device_stats TSA_GUARDED_BY(data_mutex);

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

    std::unordered_map<String /* device name */, NetworkInterfaceStatValues> network_interface_stats TSA_GUARDED_BY(data_mutex);

    Stopwatch block_devices_rescan_delay TSA_GUARDED_BY(data_mutex);

    void openSensors();
    void openBlockDevices();
    void openSensorsChips();
    void openEDAC();

    void applyCPUMetricsUpdate(
        AsynchronousMetricValues & new_values, const std::string & cpu_suffix, const ProcStatValuesCPU & delta_values, double multiplier);

    void applyNormalizedCPUMetricsUpdate(
        AsynchronousMetricValues & new_values,
        double num_cpus_to_normalize,
        const ProcStatValuesCPU & delta_values_all_cpus,
        double multiplier);
#endif

    void run();
};

}
