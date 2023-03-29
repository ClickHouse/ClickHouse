#include <Common/AsynchronousMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/filesystemHelpers.h>
#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>
#include <IO/ReadHelpers.h>
#include <base/errnoToString.h>
#include <chrono>

#include "config.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int CANNOT_SYSCONF;
}


#if defined(OS_LINUX)

static constexpr size_t small_buffer_size = 4096;

static void openFileIfExists(const char * filename, std::optional<ReadBufferFromFilePRead> & out)
{
    /// Ignoring time of check is not time of use cases, as procfs/sysfs files are fairly persistent.

    std::error_code ec;
    if (std::filesystem::is_regular_file(filename, ec))
        out.emplace(filename, small_buffer_size);
}

static std::unique_ptr<ReadBufferFromFilePRead> openFileIfExists(const std::string & filename)
{
    std::error_code ec;
    if (std::filesystem::is_regular_file(filename, ec))
        return std::make_unique<ReadBufferFromFilePRead>(filename, small_buffer_size);
    return {};
}

#endif


AsynchronousMetrics::AsynchronousMetrics(
    int update_period_seconds,
    const ProtocolServerMetricsFunc & protocol_server_metrics_func_)
    : update_period(update_period_seconds)
    , log(&Poco::Logger::get("AsynchronousMetrics"))
    , protocol_server_metrics_func(protocol_server_metrics_func_)
{
#if defined(OS_LINUX)
    openFileIfExists("/proc/meminfo", meminfo);
    openFileIfExists("/proc/loadavg", loadavg);
    openFileIfExists("/proc/stat", proc_stat);
    openFileIfExists("/proc/cpuinfo", cpuinfo);
    openFileIfExists("/proc/sys/fs/file-nr", file_nr);
    openFileIfExists("/proc/uptime", uptime);
    openFileIfExists("/proc/net/dev", net_dev);

    openFileIfExists("/sys/fs/cgroup/memory/memory.limit_in_bytes", cgroupmem_limit_in_bytes);
    openFileIfExists("/sys/fs/cgroup/memory/memory.usage_in_bytes", cgroupmem_usage_in_bytes);

    openSensors();
    openBlockDevices();
    openEDAC();
    openSensorsChips();
#endif
}

#if defined(OS_LINUX)
void AsynchronousMetrics::openSensors()
{
    LOG_TRACE(log, "Scanning /sys/class/thermal");

    thermal.clear();

    for (size_t thermal_device_index = 0;; ++thermal_device_index)
    {
        std::unique_ptr<ReadBufferFromFilePRead> file = openFileIfExists(fmt::format("/sys/class/thermal/thermal_zone{}/temp", thermal_device_index));
        if (!file)
        {
            /// Sometimes indices are from zero sometimes from one.
            if (thermal_device_index == 0)
                continue;
            else
                break;
        }

        file->rewind();
        Int64 temperature = 0;
        try
        {
            readText(temperature, *file);
        }
        catch (const ErrnoException & e)
        {
            LOG_WARNING(
                &Poco::Logger::get("AsynchronousMetrics"),
                "Thermal monitor '{}' exists but could not be read: {}.",
                thermal_device_index,
                errnoToString(e.getErrno()));
            continue;
        }

        thermal.emplace_back(std::move(file));
    }
}

void AsynchronousMetrics::openBlockDevices()
{
    LOG_TRACE(log, "Scanning /sys/block");

    if (!std::filesystem::exists("/sys/block"))
        return;

    block_devices_rescan_delay.restart();

    block_devs.clear();

    for (const auto & device_dir : std::filesystem::directory_iterator("/sys/block"))
    {
        String device_name = device_dir.path().filename();

        /// We are not interested in loopback devices.
        if (device_name.starts_with("loop"))
            continue;

        std::unique_ptr<ReadBufferFromFilePRead> file = openFileIfExists(device_dir.path() / "stat");
        if (!file)
            continue;

        block_devs[device_name] = std::move(file);
    }
}

void AsynchronousMetrics::openEDAC()
{
    LOG_TRACE(log, "Scanning /sys/devices/system/edac");

    edac.clear();

    for (size_t edac_index = 0;; ++edac_index)
    {
        String edac_correctable_file = fmt::format("/sys/devices/system/edac/mc/mc{}/ce_count", edac_index);
        String edac_uncorrectable_file = fmt::format("/sys/devices/system/edac/mc/mc{}/ue_count", edac_index);

        bool edac_correctable_file_exists = std::filesystem::exists(edac_correctable_file);
        bool edac_uncorrectable_file_exists = std::filesystem::exists(edac_uncorrectable_file);

        if (!edac_correctable_file_exists && !edac_uncorrectable_file_exists)
        {
            if (edac_index == 0)
                continue;
            else
                break;
        }

        edac.emplace_back();

        if (edac_correctable_file_exists)
            edac.back().first = openFileIfExists(edac_correctable_file);
        if (edac_uncorrectable_file_exists)
            edac.back().second = openFileIfExists(edac_uncorrectable_file);
    }
}

void AsynchronousMetrics::openSensorsChips()
{
    LOG_TRACE(log, "Scanning /sys/class/hwmon");

    hwmon_devices.clear();

    for (size_t hwmon_index = 0;; ++hwmon_index)
    {
        String hwmon_name_file = fmt::format("/sys/class/hwmon/hwmon{}/name", hwmon_index);
        if (!std::filesystem::exists(hwmon_name_file))
        {
            if (hwmon_index == 0)
                continue;
            else
                break;
        }

        String hwmon_name;
        ReadBufferFromFilePRead hwmon_name_in(hwmon_name_file, small_buffer_size);
        readText(hwmon_name, hwmon_name_in);
        std::replace(hwmon_name.begin(), hwmon_name.end(), ' ', '_');

        for (size_t sensor_index = 0;; ++sensor_index)
        {
            String sensor_name_file = fmt::format("/sys/class/hwmon/hwmon{}/temp{}_label", hwmon_index, sensor_index);
            String sensor_value_file = fmt::format("/sys/class/hwmon/hwmon{}/temp{}_input", hwmon_index, sensor_index);

            bool sensor_name_file_exists = std::filesystem::exists(sensor_name_file);
            bool sensor_value_file_exists = std::filesystem::exists(sensor_value_file);

            /// Sometimes there are labels but there is no files with data or vice versa.
            if (!sensor_name_file_exists && !sensor_value_file_exists)
            {
                if (sensor_index == 0)
                    continue;
                else
                    break;
            }

            std::unique_ptr<ReadBufferFromFilePRead> file = openFileIfExists(sensor_value_file);
            if (!file)
                continue;

            String sensor_name{};
            try
            {
                if (sensor_name_file_exists)
                {
                    ReadBufferFromFilePRead sensor_name_in(sensor_name_file, small_buffer_size);
                    readText(sensor_name, sensor_name_in);
                    std::replace(sensor_name.begin(), sensor_name.end(), ' ', '_');
                }

                file->rewind();
                Int64 temperature = 0;
                readText(temperature, *file);
            }
            catch (const ErrnoException & e)
            {
                LOG_WARNING(
                    &Poco::Logger::get("AsynchronousMetrics"),
                    "Hardware monitor '{}', sensor '{}' exists but could not be read: {}.",
                    hwmon_name,
                    sensor_index,
                    errnoToString(e.getErrno()));
                continue;
            }

            hwmon_devices[hwmon_name][sensor_name] = std::move(file);
        }
    }
}
#endif

void AsynchronousMetrics::start()
{
    /// Update once right now, to make metrics available just after server start
    /// (without waiting for asynchronous_metrics_update_period_s).
    update(std::chrono::system_clock::now());
    thread = std::make_unique<ThreadFromGlobalPool>([this] { run(); });
}

void AsynchronousMetrics::stop()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        wait_cond.notify_one();
        if (thread)
        {
            thread->join();
            thread.reset();
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

AsynchronousMetrics::~AsynchronousMetrics()
{
    stop();
}


AsynchronousMetricValues AsynchronousMetrics::getValues() const
{
    std::lock_guard lock{mutex};
    return values;
}

static auto get_next_update_time(std::chrono::seconds update_period)
{
    using namespace std::chrono;

    const auto now = time_point_cast<seconds>(system_clock::now());

    // Use seconds since the start of the hour, because we don't know when
    // the epoch started, maybe on some weird fractional time.
    const auto start_of_hour = time_point_cast<seconds>(time_point_cast<hours>(now));
    const auto seconds_passed = now - start_of_hour;

    // Rotate time forward by half a period -- e.g. if a period is a minute,
    // we'll collect metrics on start of minute + 30 seconds. This is to
    // achieve temporal separation with MetricTransmitter. Don't forget to
    // rotate it back.
    const auto rotation = update_period / 2;

    const auto periods_passed = (seconds_passed + rotation) / update_period;
    const auto seconds_next = (periods_passed + 1) * update_period - rotation;
    const auto time_next = start_of_hour + seconds_next;

    return time_next;
}

void AsynchronousMetrics::run()
{
    setThreadName("AsyncMetrics");

    while (true)
    {
        auto next_update_time = get_next_update_time(update_period);

        {
            // Wait first, so that the first metric collection is also on even time.
            std::unique_lock lock{mutex};
            if (wait_cond.wait_until(lock, next_update_time,
                [this] { return quit; }))
            {
                break;
            }
        }

        try
        {
            update(next_update_time);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

#if USE_JEMALLOC
uint64_t updateJemallocEpoch()
{
    uint64_t value = 0;
    size_t size = sizeof(value);
    mallctl("epoch", &value, &size, &value, size);
    return value;
}

template <typename Value>
static Value saveJemallocMetricImpl(
    AsynchronousMetricValues & values,
    const std::string & jemalloc_full_name,
    const std::string & clickhouse_full_name)
{
    Value value{};
    size_t size = sizeof(value);
    mallctl(jemalloc_full_name.c_str(), &value, &size, nullptr, 0);
    values[clickhouse_full_name] = AsynchronousMetricValue(value, "An internal metric of the low-level memory allocator (jemalloc). See https://jemalloc.net/jemalloc.3.html");
    return value;
}

template<typename Value>
static Value saveJemallocMetric(AsynchronousMetricValues & values,
    const std::string & metric_name)
{
    return saveJemallocMetricImpl<Value>(values,
        fmt::format("stats.{}", metric_name),
        fmt::format("jemalloc.{}", metric_name));
}

template<typename Value>
static Value saveAllArenasMetric(AsynchronousMetricValues & values,
    const std::string & metric_name)
{
    return saveJemallocMetricImpl<Value>(values,
        fmt::format("stats.arenas.{}.{}", MALLCTL_ARENAS_ALL, metric_name),
        fmt::format("jemalloc.arenas.all.{}", metric_name));
}
#endif


#if defined(OS_LINUX)

void AsynchronousMetrics::ProcStatValuesCPU::read(ReadBuffer & in)
{
    readText(user, in);
    skipWhitespaceIfAny(in, true);
    readText(nice, in);
    skipWhitespaceIfAny(in, true);
    readText(system, in);
    skipWhitespaceIfAny(in, true);
    readText(idle, in);
    skipWhitespaceIfAny(in, true);
    readText(iowait, in);
    skipWhitespaceIfAny(in, true);
    readText(irq, in);
    skipWhitespaceIfAny(in, true);
    readText(softirq, in);

    /// Just in case for old Linux kernels, we check if these values present.

    if (!checkChar('\n', in))
    {
        skipWhitespaceIfAny(in, true);
        readText(steal, in);
    }

    if (!checkChar('\n', in))
    {
        skipWhitespaceIfAny(in, true);
        readText(guest, in);
    }

    if (!checkChar('\n', in))
    {
        skipWhitespaceIfAny(in, true);
        readText(guest_nice, in);
    }

    skipToNextLineOrEOF(in);
}

AsynchronousMetrics::ProcStatValuesCPU
AsynchronousMetrics::ProcStatValuesCPU::operator-(const AsynchronousMetrics::ProcStatValuesCPU & other) const
{
    ProcStatValuesCPU res{};
    res.user = user - other.user;
    res.nice = nice - other.nice;
    res.system = system - other.system;
    res.idle = idle - other.idle;
    res.iowait = iowait - other.iowait;
    res.irq = irq - other.irq;
    res.softirq = softirq - other.softirq;
    res.steal = steal - other.steal;
    res.guest = guest - other.guest;
    res.guest_nice = guest_nice - other.guest_nice;
    return res;
}

AsynchronousMetrics::ProcStatValuesOther
AsynchronousMetrics::ProcStatValuesOther::operator-(const AsynchronousMetrics::ProcStatValuesOther & other) const
{
    ProcStatValuesOther res{};
    res.interrupts = interrupts - other.interrupts;
    res.context_switches = context_switches - other.context_switches;
    res.processes_created = processes_created - other.processes_created;
    return res;
}

void AsynchronousMetrics::BlockDeviceStatValues::read(ReadBuffer & in)
{
    skipWhitespaceIfAny(in, true);
    readText(read_ios, in);
    skipWhitespaceIfAny(in, true);
    readText(read_merges, in);
    skipWhitespaceIfAny(in, true);
    readText(read_sectors, in);
    skipWhitespaceIfAny(in, true);
    readText(read_ticks, in);
    skipWhitespaceIfAny(in, true);
    readText(write_ios, in);
    skipWhitespaceIfAny(in, true);
    readText(write_merges, in);
    skipWhitespaceIfAny(in, true);
    readText(write_sectors, in);
    skipWhitespaceIfAny(in, true);
    readText(write_ticks, in);
    skipWhitespaceIfAny(in, true);
    readText(in_flight_ios, in);
    skipWhitespaceIfAny(in, true);
    readText(io_ticks, in);
    skipWhitespaceIfAny(in, true);
    readText(time_in_queue, in);
    skipWhitespaceIfAny(in, true);
    readText(discard_ops, in);
    skipWhitespaceIfAny(in, true);
    readText(discard_merges, in);
    skipWhitespaceIfAny(in, true);
    readText(discard_sectors, in);
    skipWhitespaceIfAny(in, true);
    readText(discard_ticks, in);
}

AsynchronousMetrics::BlockDeviceStatValues
AsynchronousMetrics::BlockDeviceStatValues::operator-(const AsynchronousMetrics::BlockDeviceStatValues & other) const
{
    BlockDeviceStatValues res{};
    res.read_ios = read_ios - other.read_ios;
    res.read_merges = read_merges - other.read_merges;
    res.read_sectors = read_sectors - other.read_sectors;
    res.read_ticks = read_ticks - other.read_ticks;
    res.write_ios = write_ios - other.write_ios;
    res.write_merges = write_merges - other.write_merges;
    res.write_sectors = write_sectors - other.write_sectors;
    res.write_ticks = write_ticks - other.write_ticks;
    res.in_flight_ios = in_flight_ios; /// This is current value, not total.
    res.io_ticks = io_ticks - other.io_ticks;
    res.time_in_queue = time_in_queue - other.time_in_queue;
    res.discard_ops = discard_ops - other.discard_ops;
    res.discard_merges = discard_merges - other.discard_merges;
    res.discard_sectors = discard_sectors - other.discard_sectors;
    res.discard_ticks = discard_ticks - other.discard_ticks;
    return res;
}

AsynchronousMetrics::NetworkInterfaceStatValues
AsynchronousMetrics::NetworkInterfaceStatValues::operator-(const AsynchronousMetrics::NetworkInterfaceStatValues & other) const
{
    NetworkInterfaceStatValues res{};
    res.recv_bytes = recv_bytes - other.recv_bytes;
    res.recv_packets = recv_packets - other.recv_packets;
    res.recv_errors = recv_errors - other.recv_errors;
    res.recv_drop = recv_drop - other.recv_drop;
    res.send_bytes = send_bytes - other.send_bytes;
    res.send_packets = send_packets - other.send_packets;
    res.send_errors = send_errors - other.send_errors;
    res.send_drop = send_drop - other.send_drop;
    return res;
}

#endif


void AsynchronousMetrics::update(TimePoint update_time)
{
    Stopwatch watch;

    AsynchronousMetricValues new_values;

    auto current_time = std::chrono::system_clock::now();
    auto time_after_previous_update = current_time - previous_update_time;
    previous_update_time = update_time;

    double update_interval = 0.;
    if (first_run)
        update_interval = update_period.count();
    else
        update_interval = std::chrono::duration_cast<std::chrono::microseconds>(time_after_previous_update).count() / 1e6;
    new_values["AsynchronousMetricsUpdateInterval"] = { update_interval, "Metrics update interval" };

    /// This is also a good indicator of system responsiveness.
    new_values["Jitter"] = { std::chrono::duration_cast<std::chrono::nanoseconds>(current_time - update_time).count() / 1e9,
        "The difference in time the thread for calculation of the asynchronous metrics was scheduled to wake up and the time it was in fact, woken up."
        " A proxy-indicator of overall system latency and responsiveness." };

#if defined(OS_LINUX) || defined(OS_FREEBSD)
    MemoryStatisticsOS::Data memory_statistics_data = memory_stat.get();
#endif

#if USE_JEMALLOC
    // 'epoch' is a special mallctl -- it updates the statistics. Without it, all
    // the following calls will return stale values. It increments and returns
    // the current epoch number, which might be useful to log as a sanity check.
    auto epoch = updateJemallocEpoch();
    new_values["jemalloc.epoch"] = { epoch, "An internal incremental update number of the statistics of jemalloc (Jason Evans' memory allocator), used in all other `jemalloc` metrics." };

    // Collect the statistics themselves.
    saveJemallocMetric<size_t>(new_values, "allocated");
    saveJemallocMetric<size_t>(new_values, "active");
    saveJemallocMetric<size_t>(new_values, "metadata");
    saveJemallocMetric<size_t>(new_values, "metadata_thp");
    saveJemallocMetric<size_t>(new_values, "resident");
    saveJemallocMetric<size_t>(new_values, "mapped");
    saveJemallocMetric<size_t>(new_values, "retained");
    saveJemallocMetric<size_t>(new_values, "background_thread.num_threads");
    saveJemallocMetric<uint64_t>(new_values, "background_thread.num_runs");
    saveJemallocMetric<uint64_t>(new_values, "background_thread.run_intervals");
    saveAllArenasMetric<size_t>(new_values, "pactive");
    [[maybe_unused]] size_t je_malloc_pdirty = saveAllArenasMetric<size_t>(new_values, "pdirty");
    [[maybe_unused]] size_t je_malloc_pmuzzy = saveAllArenasMetric<size_t>(new_values, "pmuzzy");
    saveAllArenasMetric<size_t>(new_values, "dirty_purged");
    saveAllArenasMetric<size_t>(new_values, "muzzy_purged");
#endif

    /// Process process memory usage according to OS
#if defined(OS_LINUX) || defined(OS_FREEBSD)
    {
        MemoryStatisticsOS::Data & data = memory_statistics_data;

        new_values["MemoryVirtual"] = { data.virt,
            "The size of the virtual address space allocated by the server process, in bytes."
            " The size of the virtual address space is usually much greater than the physical memory consumption, and should not be used as an estimate for the memory consumption."
            " The large values of this metric are totally normal, and makes only technical sense."};
        new_values["MemoryResident"] = { data.resident,
            "The amount of physical memory used by the server process, in bytes." };
#if !defined(OS_FREEBSD)
        new_values["MemoryShared"] = { data.shared,
            "The amount of memory used by the server process, that is also shared by another processes, in bytes."
            " ClickHouse does not use shared memory, but some memory can be labeled by OS as shared for its own reasons."
            " This metric does not make a lot of sense to watch, and it exists only for completeness reasons."};
#endif
        new_values["MemoryCode"] = { data.code,
            "The amount of virtual memory mapped for the pages of machine code of the server process, in bytes." };
        new_values["MemoryDataAndStack"] = { data.data_and_stack,
            "The amount of virtual memory mapped for the use of stack and for the allocated memory, in bytes."
            " It is unspecified whether it includes the per-thread stacks and most of the allocated memory, that is allocated with the 'mmap' system call."
            " This metric exists only for completeness reasons. I recommend to use the `MemoryResident` metric for monitoring."};

        /// We must update the value of total_memory_tracker periodically.
        /// Otherwise it might be calculated incorrectly - it can include a "drift" of memory amount.
        /// See https://github.com/ClickHouse/ClickHouse/issues/10293
        {
            Int64 amount = total_memory_tracker.get();
            Int64 peak = total_memory_tracker.getPeak();
            Int64 rss = data.resident;
            Int64 free_memory_in_allocator_arenas = 0;

#if USE_JEMALLOC
            /// According to jemalloc man, pdirty is:
            ///
            ///     Number of pages within unused extents that are potentially
            ///     dirty, and for which madvise() or similar has not been called.
            ///
            /// So they will be subtracted from RSS to make accounting more
            /// accurate, since those pages are not really RSS but a memory
            /// that can be used at anytime via jemalloc.
            free_memory_in_allocator_arenas = je_malloc_pdirty * getPageSize();
#endif

            Int64 difference = rss - amount;

            /// Log only if difference is high. This is for convenience. The threshold is arbitrary.
            if (difference >= 1048576 || difference <= -1048576)
                LOG_TRACE(log,
                    "MemoryTracking: was {}, peak {}, free memory in arenas {}, will set to {} (RSS), difference: {}",
                    ReadableSize(amount),
                    ReadableSize(peak),
                    ReadableSize(free_memory_in_allocator_arenas),
                    ReadableSize(rss),
                    ReadableSize(difference));

            total_memory_tracker.setRSS(rss, free_memory_in_allocator_arenas);
        }
    }
#endif

#if defined(OS_LINUX)
    if (loadavg)
    {
        try
        {
            loadavg->rewind();

            Float64 loadavg1 = 0;
            Float64 loadavg5 = 0;
            Float64 loadavg15 = 0;
            UInt64 threads_runnable = 0;
            UInt64 threads_total = 0;

            readText(loadavg1, *loadavg);
            skipWhitespaceIfAny(*loadavg);
            readText(loadavg5, *loadavg);
            skipWhitespaceIfAny(*loadavg);
            readText(loadavg15, *loadavg);
            skipWhitespaceIfAny(*loadavg);
            readText(threads_runnable, *loadavg);
            assertChar('/', *loadavg);
            readText(threads_total, *loadavg);

#define LOAD_AVERAGE_DOCUMENTATION \
    " The load represents the number of threads across all the processes (the scheduling entities of the OS kernel)," \
    " that are currently running by CPU or waiting for IO, or ready to run but not being scheduled at this point of time." \
    " This number includes all the processes, not only clickhouse-server. The number can be greater than the number of CPU cores," \
    " if the system is overloaded, and many processes are ready to run but waiting for CPU or IO."

            new_values["LoadAverage1"] = { loadavg1,
                "The whole system load, averaged with exponential smoothing over 1 minute." LOAD_AVERAGE_DOCUMENTATION };
            new_values["LoadAverage5"] = { loadavg5,
                "The whole system load, averaged with exponential smoothing over 5 minutes." LOAD_AVERAGE_DOCUMENTATION };
            new_values["LoadAverage15"] = { loadavg15,
                "The whole system load, averaged with exponential smoothing over 15 minutes." LOAD_AVERAGE_DOCUMENTATION };
            new_values["OSThreadsRunnable"] = { threads_runnable,
                "The total number of 'runnable' threads, as the OS kernel scheduler seeing it." };
            new_values["OSThreadsTotal"] = { threads_total,
                "The total number of threads, as the OS kernel scheduler seeing it." };
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (uptime)
    {
        try
        {
            uptime->rewind();

            Float64 uptime_seconds = 0;
            readText(uptime_seconds, *uptime);

            new_values["OSUptime"] = { uptime_seconds, "The uptime of the host server (the machine where ClickHouse is running), in seconds." };
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (proc_stat)
    {
        try
        {
            proc_stat->rewind();

            int64_t hz = sysconf(_SC_CLK_TCK);
            if (-1 == hz)
                throwFromErrno("Cannot call 'sysconf' to obtain system HZ", ErrorCodes::CANNOT_SYSCONF);

            double multiplier = 1.0 / hz / (std::chrono::duration_cast<std::chrono::nanoseconds>(time_after_previous_update).count() / 1e9);
            size_t num_cpus = 0;

            ProcStatValuesOther current_other_values{};
            ProcStatValuesCPU delta_values_all_cpus{};

            while (!proc_stat->eof())
            {
                String name;
                readStringUntilWhitespace(name, *proc_stat);
                skipWhitespaceIfAny(*proc_stat);

                if (name.starts_with("cpu"))
                {
                    String cpu_num_str = name.substr(strlen("cpu"));
                    UInt64 cpu_num = 0;
                    if (!cpu_num_str.empty())
                    {
                        cpu_num = parse<UInt64>(cpu_num_str);

                        if (cpu_num > 1000000) /// Safety check, arbitrary large number, suitable for supercomputing applications.
                            throw Exception(ErrorCodes::CORRUPTED_DATA, "Too many CPUs (at least {}) in '/proc/stat' file", cpu_num);

                        if (proc_stat_values_per_cpu.size() <= cpu_num)
                            proc_stat_values_per_cpu.resize(cpu_num + 1);
                    }

                    ProcStatValuesCPU current_values{};
                    current_values.read(*proc_stat);

                    ProcStatValuesCPU & prev_values = !cpu_num_str.empty() ? proc_stat_values_per_cpu[cpu_num] : proc_stat_values_all_cpus;

                    if (!first_run)
                    {
                        ProcStatValuesCPU delta_values = current_values - prev_values;

                        String cpu_suffix;
                        if (!cpu_num_str.empty())
                        {
                            cpu_suffix = "CPU" + cpu_num_str;
                            ++num_cpus;
                        }
                        else
                            delta_values_all_cpus = delta_values;

                        new_values["OSUserTime" + cpu_suffix] = { delta_values.user * multiplier,
                            "The ratio of time the CPU core was running userspace code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " This includes also the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core)."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSNiceTime" + cpu_suffix] = { delta_values.nice * multiplier,
                            "The ratio of time the CPU core was running userspace code with higher priority. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSSystemTime" + cpu_suffix] = { delta_values.system * multiplier,
                            "The ratio of time the CPU core was running OS kernel (system) code. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSIdleTime" + cpu_suffix] = { delta_values.idle * multiplier,
                            "The ratio of time the CPU core was idle (not even ready to run a process waiting for IO) from the OS kernel standpoint. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " This does not include the time when the CPU was under-utilized due to the reasons internal to the CPU (memory loads, pipeline stalls, branch mispredictions, running another SMT core)."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSIOWaitTime" + cpu_suffix] = { delta_values.iowait * multiplier,
                            "The ratio of time the CPU core was not running the code but when the OS kernel did not run any other process on this CPU as the processes were waiting for IO. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSIrqTime" + cpu_suffix] = { delta_values.irq * multiplier,
                            "The ratio of time spent for running hardware interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " A high number of this metric may indicate hardware misconfiguration or a very high network load."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSSoftIrqTime" + cpu_suffix] = { delta_values.softirq * multiplier,
                            "The ratio of time spent for running software interrupt requests on the CPU. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " A high number of this metric may indicate inefficient software running on the system."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSStealTime" + cpu_suffix] = { delta_values.steal * multiplier,
                            "The ratio of time spent in other operating systems by the CPU when running in a virtualized environment. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " Not every virtualized environments present this metric, and most of them don't."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSGuestTime" + cpu_suffix] = { delta_values.guest * multiplier,
                            "The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " This metric is irrelevant for ClickHouse, but still exists for completeness."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                        new_values["OSGuestNiceTime" + cpu_suffix] = { delta_values.guest_nice * multiplier,
                            "The ratio of time spent running a virtual CPU for guest operating systems under the control of the Linux kernel, when a guest was set to a higher priority (See `man procfs`). This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server."
                            " This metric is irrelevant for ClickHouse, but still exists for completeness."
                            " The value for a single CPU core will be in the interval [0..1]. The value for all CPU cores is calculated as a sum across them [0..num cores]."};
                    }

                    prev_values = current_values;
                }
                else if (name == "intr")
                {
                    readText(current_other_values.interrupts, *proc_stat);
                    skipToNextLineOrEOF(*proc_stat);
                }
                else if (name == "ctxt")
                {
                    readText(current_other_values.context_switches, *proc_stat);
                    skipToNextLineOrEOF(*proc_stat);
                }
                else if (name == "processes")
                {
                    readText(current_other_values.processes_created, *proc_stat);
                    skipToNextLineOrEOF(*proc_stat);
                }
                else if (name == "procs_running")
                {
                    UInt64 processes_running = 0;
                    readText(processes_running, *proc_stat);
                    skipToNextLineOrEOF(*proc_stat);
                    new_values["OSProcessesRunning"] = { processes_running,
                        "The number of runnable (running or ready to run) threads by the operating system."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
                else if (name == "procs_blocked")
                {
                    UInt64 processes_blocked = 0;
                    readText(processes_blocked, *proc_stat);
                    skipToNextLineOrEOF(*proc_stat);
                    new_values["OSProcessesBlocked"] = { processes_blocked,
                        "Number of threads blocked waiting for I/O to complete (`man procfs`)."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
                else
                    skipToNextLineOrEOF(*proc_stat);
            }

            if (!first_run)
            {
                ProcStatValuesOther delta_values = current_other_values - proc_stat_values_other;

                new_values["OSInterrupts"] = { delta_values.interrupts, "The number of interrupts on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                new_values["OSContextSwitches"] = { delta_values.context_switches, "The number of context switches that the system underwent on the host machine. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                new_values["OSProcessesCreated"] = { delta_values.processes_created, "The number of processes created. This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };

                /// Also write values normalized to 0..1 by diving to the number of CPUs.
                /// These values are good to be averaged across the cluster of non-uniform servers.

                if (num_cpus)
                {
                    new_values["OSUserTimeNormalized"] = { delta_values_all_cpus.user * multiplier / num_cpus,
                        "The value is similar to `OSUserTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSNiceTimeNormalized"] = { delta_values_all_cpus.nice * multiplier / num_cpus,
                        "The value is similar to `OSNiceTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSSystemTimeNormalized"] = { delta_values_all_cpus.system * multiplier / num_cpus,
                        "The value is similar to `OSSystemTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSIdleTimeNormalized"] = { delta_values_all_cpus.idle * multiplier / num_cpus,
                        "The value is similar to `OSIdleTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSIOWaitTimeNormalized"] = { delta_values_all_cpus.iowait * multiplier / num_cpus,
                        "The value is similar to `OSIOWaitTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSIrqTimeNormalized"] = { delta_values_all_cpus.irq * multiplier / num_cpus,
                        "The value is similar to `OSIrqTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSSoftIrqTimeNormalized"] = { delta_values_all_cpus.softirq * multiplier / num_cpus,
                        "The value is similar to `OSSoftIrqTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSStealTimeNormalized"] = { delta_values_all_cpus.steal * multiplier / num_cpus,
                        "The value is similar to `OSStealTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSGuestTimeNormalized"] = { delta_values_all_cpus.guest * multiplier / num_cpus,
                        "The value is similar to `OSGuestTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                    new_values["OSGuestNiceTimeNormalized"] = { delta_values_all_cpus.guest_nice * multiplier / num_cpus,
                        "The value is similar to `OSGuestNiceTime` but divided to the number of CPU cores to be measured in the [0..1] interval regardless of the number of cores."
                        " This allows you to average the values of this metric across multiple servers in a cluster even if the number of cores is non-uniform, and still get the average resource utilization metric."};
                }
            }

            proc_stat_values_other = current_other_values;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (cgroupmem_limit_in_bytes && cgroupmem_usage_in_bytes)
    {
        try {
            cgroupmem_limit_in_bytes->rewind();
            cgroupmem_usage_in_bytes->rewind();

            uint64_t cgroup_mem_limit_in_bytes = 0;
            uint64_t cgroup_mem_usage_in_bytes = 0;

            readText(cgroup_mem_limit_in_bytes, *cgroupmem_limit_in_bytes);
            readText(cgroup_mem_usage_in_bytes, *cgroupmem_usage_in_bytes);

            if (cgroup_mem_limit_in_bytes && cgroup_mem_usage_in_bytes)
            {
                new_values["CgroupMemoryTotal"] = { cgroup_mem_limit_in_bytes, "The total amount of memory in cgroup, in bytes." };
                new_values["CgroupMemoryUsed"] = { cgroup_mem_usage_in_bytes, "The amount of memory used in cgroup, in bytes." };
            }
            else
            {
                LOG_DEBUG(log, "Cannot read statistics about the cgroup memory total and used. Total got '{}', Used got '{}'.",
                    cgroup_mem_limit_in_bytes, cgroup_mem_usage_in_bytes);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (meminfo)
    {
        try
        {
            meminfo->rewind();

            uint64_t free_plus_cached_bytes = 0;

            while (!meminfo->eof())
            {
                String name;
                readStringUntilWhitespace(name, *meminfo);
                skipWhitespaceIfAny(*meminfo, true);

                uint64_t kb = 0;
                readText(kb, *meminfo);

                if (!kb)
                {
                    skipToNextLineOrEOF(*meminfo);
                    continue;
                }

                skipWhitespaceIfAny(*meminfo, true);

                /**
                 * Not all entries in /proc/meminfo contain the kB suffix, e.g.
                 * HugePages_Total:       0
                 * HugePages_Free:        0
                 * We simply skip such entries as they're not needed
                 */
                if (*meminfo->position() == '\n')
                {
                    skipToNextLineOrEOF(*meminfo);
                    continue;
                }

                assertString("kB", *meminfo);

                uint64_t bytes = kb * 1024;

                if (name == "MemTotal:")
                {
                    new_values["OSMemoryTotal"] = { bytes, "The total amount of memory on the host system, in bytes." };
                }
                else if (name == "MemFree:")
                {
                    free_plus_cached_bytes += bytes;
                    new_values["OSMemoryFreeWithoutCached"] = { bytes,
                        "The amount of free memory on the host system, in bytes."
                        " This does not include the memory used by the OS page cache memory, in bytes."
                        " The page cache memory is also available for usage by programs, so the value of this metric can be confusing."
                        " See the `OSMemoryAvailable` metric instead."
                        " For convenience we also provide the `OSMemoryFreePlusCached` metric, that should be somewhat similar to OSMemoryAvailable."
                        " See also https://www.linuxatemyram.com/."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
                else if (name == "MemAvailable:")
                {
                    new_values["OSMemoryAvailable"] = { bytes, "The amount of memory available to be used by programs, in bytes. This is very similar to the `OSMemoryFreePlusCached` metric."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
                else if (name == "Buffers:")
                {
                    new_values["OSMemoryBuffers"] = { bytes, "The amount of memory used by OS kernel buffers, in bytes. This should be typically small, and large values may indicate a misconfiguration of the OS."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
                else if (name == "Cached:")
                {
                    free_plus_cached_bytes += bytes;
                    new_values["OSMemoryCached"] = { bytes, "The amount of memory used by the OS page cache, in bytes. Typically, almost all available memory is used by the OS page cache - high values of this metric are normal and expected."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
                else if (name == "SwapCached:")
                {
                    new_values["OSMemorySwapCached"] = { bytes, "The amount of memory in swap that was also loaded in RAM. Swap should be disabled on production systems. If the value of this metric is large, it indicates a misconfiguration."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }

                skipToNextLineOrEOF(*meminfo);
            }

            new_values["OSMemoryFreePlusCached"] = { free_plus_cached_bytes, "The amount of free memory plus OS page cache memory on the host system, in bytes. This memory is available to be used by programs. The value should be very similar to `OSMemoryAvailable`."
                " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    // Try to add processor frequencies, ignoring errors.
    if (cpuinfo)
    {
        try
        {
            cpuinfo->rewind();

            // We need the following lines:
            // processor : 4
            // cpu MHz : 4052.941
            // They contain tabs and are interspersed with other info.

            int core_id = 0;
            while (!cpuinfo->eof())
            {
                std::string s;
                // We don't have any backslash escape sequences in /proc/cpuinfo, so
                // this function will read the line until EOL, which is exactly what
                // we need.
                readEscapedStringUntilEOL(s, *cpuinfo);
                // It doesn't read the EOL itself.
                ++cpuinfo->position();

                if (s.rfind("processor", 0) == 0)
                {
                    /// s390x example: processor 0: version = FF, identification = 039C88, machine = 3906
                    /// non s390x example: processor : 0
                    if (auto colon = s.find_first_of(':'))
                    {
#ifdef __s390x__
                        core_id = std::stoi(s.substr(10)); /// 10: length of "processor" plus 1
#else
                        core_id = std::stoi(s.substr(colon + 2));
#endif
                    }
                }
                else if (s.rfind("cpu MHz", 0) == 0)
                {
                    if (auto colon = s.find_first_of(':'))
                    {
                        auto mhz = std::stod(s.substr(colon + 2));
                        new_values[fmt::format("CPUFrequencyMHz_{}", core_id)] = { mhz, "The current frequency of the CPU, in MHz. Most of the modern CPUs adjust the frequency dynamically for power saving and Turbo Boosting." };
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (file_nr)
    {
        try
        {
            file_nr->rewind();

            uint64_t open_files = 0;
            readText(open_files, *file_nr);
            new_values["OSOpenFiles"] = { open_files, "The total number of opened files on the host machine."
                " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// Update list of block devices periodically
    /// (i.e. someone may add new disk to RAID array)
    if (block_devices_rescan_delay.elapsedSeconds() >= 300)
        openBlockDevices();

    try
    {
        for (auto & [name, device] : block_devs)
        {
            device->rewind();

            BlockDeviceStatValues current_values{};
            BlockDeviceStatValues & prev_values = block_device_stats[name];

            try
            {
                current_values.read(*device);
            }
            catch (const ErrnoException & e)
            {
                LOG_DEBUG(log, "Cannot read statistics about the block device '{}': {}.",
                    name, errnoToString(e.getErrno()));
                continue;
            }

            BlockDeviceStatValues delta_values = current_values - prev_values;
            prev_values = current_values;

            if (first_run)
                continue;

            /// Always 512 according to the docs.
            static constexpr size_t sector_size = 512;

            /// Always in milliseconds according to the docs.
            static constexpr double time_multiplier = 1e-6;

#define BLOCK_DEVICE_EXPLANATION \
    " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." \
    " Source: `/sys/block`. See https://www.kernel.org/doc/Documentation/block/stat.txt"

            new_values["BlockReadOps_" + name] = { delta_values.read_ios,
                "Number of read operations requested from the block device."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockWriteOps_" + name] = { delta_values.write_ios,
                "Number of write operations requested from the block device."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockDiscardOps_" + name] = { delta_values.discard_ops,
                "Number of discard operations requested from the block device. These operations are relevant for SSD."
                " Discard operations are not used by ClickHouse, but can be used by other processes on the system."
                BLOCK_DEVICE_EXPLANATION };

            new_values["BlockReadMerges_" + name] = { delta_values.read_merges,
                "Number of read operations requested from the block device and merged together by the OS IO scheduler."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockWriteMerges_" + name] = { delta_values.write_merges,
                "Number of write operations requested from the block device and merged together by the OS IO scheduler."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockDiscardMerges_" + name] = { delta_values.discard_merges,
                "Number of discard operations requested from the block device and merged together by the OS IO scheduler."
                " These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system."
                BLOCK_DEVICE_EXPLANATION };

            new_values["BlockReadBytes_" + name] = { delta_values.read_sectors * sector_size,
                "Number of bytes read from the block device."
                " It can be lower than the number of bytes read from the filesystem due to the usage of the OS page cache, that saves IO."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockWriteBytes_" + name] = { delta_values.write_sectors * sector_size,
                "Number of bytes written to the block device."
                " It can be lower than the number of bytes written to the filesystem due to the usage of the OS page cache, that saves IO."
                " A write to the block device may happen later than the corresponding write to the filesystem due to write-through caching."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockDiscardBytes_" + name] = { delta_values.discard_sectors * sector_size,
                "Number of discarded bytes on the block device."
                " These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system."
                BLOCK_DEVICE_EXPLANATION };

            new_values["BlockReadTime_" + name] = { delta_values.read_ticks * time_multiplier,
                "Time in seconds spend in read operations requested from the block device, summed across all the operations."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockWriteTime_" + name] = { delta_values.write_ticks * time_multiplier,
                "Time in seconds spend in write operations requested from the block device, summed across all the operations."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockDiscardTime_" + name] = { delta_values.discard_ticks * time_multiplier,
                "Time in seconds spend in discard operations requested from the block device, summed across all the operations."
                " These operations are relevant for SSD. Discard operations are not used by ClickHouse, but can be used by other processes on the system."
                BLOCK_DEVICE_EXPLANATION };

            new_values["BlockInFlightOps_" + name] = { delta_values.in_flight_ios,
                "This value counts the number of I/O requests that have been issued to"
                " the device driver but have not yet completed. It does not include IO"
                " requests that are in the queue but not yet issued to the device driver."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockActiveTime_" + name] = { delta_values.io_ticks * time_multiplier,
                "Time in seconds the block device had the IO requests queued."
                BLOCK_DEVICE_EXPLANATION };
            new_values["BlockQueueTime_" + name] = { delta_values.time_in_queue * time_multiplier,
                "This value counts the number of milliseconds that IO requests have waited"
                " on this block device. If there are multiple IO requests waiting, this"
                " value will increase as the product of the number of milliseconds times the"
                " number of requests waiting."
                BLOCK_DEVICE_EXPLANATION };

            if (delta_values.in_flight_ios)
            {
                /// TODO Check if these values are meaningful.

                new_values["BlockActiveTimePerOp_" + name] = { delta_values.io_ticks * time_multiplier / delta_values.in_flight_ios,
                    "Similar to the `BlockActiveTime` metrics, but the value is divided to the number of IO operations to count the per-operation time." };
                new_values["BlockQueueTimePerOp_" + name] = { delta_values.time_in_queue * time_multiplier / delta_values.in_flight_ios,
                    "Similar to the `BlockQueueTime` metrics, but the value is divided to the number of IO operations to count the per-operation time." };
            }
        }
    }
    catch (...)
    {
        LOG_DEBUG(log, "Cannot read statistics from block devices: {}", getCurrentExceptionMessage(false));

        /// Try to reopen block devices in case of error
        /// (i.e. ENOENT or ENODEV means that some disk had been replaced, and it may appear with a new name)
        try
        {
            openBlockDevices();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (net_dev)
    {
        try
        {
            net_dev->rewind();

            /// Skip first two lines:
            /// Inter-|   Receive                                                |  Transmit
            ///  face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed

            skipToNextLineOrEOF(*net_dev);
            skipToNextLineOrEOF(*net_dev);

            while (!net_dev->eof())
            {
                skipWhitespaceIfAny(*net_dev, true);
                String interface_name;
                readStringUntilWhitespace(interface_name, *net_dev);

                /// We are not interested in loopback devices.
                if (!interface_name.ends_with(':') || interface_name == "lo:" || interface_name.size() <= 1)
                {
                    skipToNextLineOrEOF(*net_dev);
                    continue;
                }

                interface_name.pop_back();

                NetworkInterfaceStatValues current_values{};
                uint64_t unused;

                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.recv_bytes, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.recv_packets, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.recv_errors, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.recv_drop, *net_dev);

                /// NOTE We should pay more attention to the number of fields.

                skipWhitespaceIfAny(*net_dev, true);
                readText(unused, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(unused, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(unused, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(unused, *net_dev);

                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.send_bytes, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.send_packets, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.send_errors, *net_dev);
                skipWhitespaceIfAny(*net_dev, true);
                readText(current_values.send_drop, *net_dev);

                skipToNextLineOrEOF(*net_dev);

                NetworkInterfaceStatValues & prev_values = network_interface_stats[interface_name];
                NetworkInterfaceStatValues delta_values = current_values - prev_values;
                prev_values = current_values;

                if (!first_run)
                {
                    new_values["NetworkReceiveBytes_" + interface_name] = { delta_values.recv_bytes,
                        " Number of bytes received via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                    new_values["NetworkReceivePackets_" + interface_name] = { delta_values.recv_packets,
                        " Number of network packets received via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                    new_values["NetworkReceiveErrors_" + interface_name] = { delta_values.recv_errors,
                        " Number of times error happened receiving via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                    new_values["NetworkReceiveDrop_" + interface_name] = { delta_values.recv_drop,
                        " Number of bytes a packet was dropped while received via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };

                    new_values["NetworkSendBytes_" + interface_name] = { delta_values.send_bytes,
                        " Number of bytes sent via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                    new_values["NetworkSendPackets_" + interface_name] = { delta_values.send_packets,
                        " Number of network packets sent via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                    new_values["NetworkSendErrors_" + interface_name] = { delta_values.send_errors,
                        " Number of times error (e.g. TCP retransmit) happened while sending via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                    new_values["NetworkSendDrop_" + interface_name] = { delta_values.send_drop,
                        " Number of times a packed was dropped while sending via the network interface."
                        " This is a system-wide metric, it includes all the processes on the host machine, not just clickhouse-server." };
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    try
    {
        for (size_t i = 0, size = thermal.size(); i < size; ++i)
        {
            ReadBufferFromFilePRead & in = *thermal[i];

            in.rewind();
            Int64 temperature = 0;
            readText(temperature, in);
            new_values[fmt::format("Temperature{}", i)] = { temperature * 0.001,
                "The temperature of the corresponding device in ℃. A sensor can return an unrealistic value. Source: `/sys/class/thermal`" };
        }
    }
    catch (...)
    {
        if (errno != ENODATA)   /// Ok for thermal sensors.
            tryLogCurrentException(__PRETTY_FUNCTION__);

        /// Files maybe re-created on module load/unload
        try
        {
            openSensors();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    try
    {
        for (const auto & [hwmon_name, sensors] : hwmon_devices)
        {
            for (const auto & [sensor_name, sensor_file] : sensors)
            {
                sensor_file->rewind();
                Int64 temperature = 0;
                try
                {
                    readText(temperature, *sensor_file);
                }
                catch (const ErrnoException & e)
                {
                    LOG_DEBUG(log, "Hardware monitor '{}', sensor '{}' exists but could not be read: {}.",
                        hwmon_name, sensor_name, errnoToString(e.getErrno()));
                    continue;
                }

                if (sensor_name.empty())
                    new_values[fmt::format("Temperature_{}", hwmon_name)] = { temperature * 0.001,
                        "The temperature reported by the corresponding hardware monitor in ℃. A sensor can return an unrealistic value. Source: `/sys/class/hwmon`" };
                else
                    new_values[fmt::format("Temperature_{}_{}", hwmon_name, sensor_name)] = { temperature * 0.001,
                        "The temperature reported by the corresponding hardware monitor and the corresponding sensor in ℃. A sensor can return an unrealistic value. Source: `/sys/class/hwmon`" };
            }
        }
    }
    catch (...)
    {
        if (errno != ENODATA)   /// Ok for thermal sensors.
            tryLogCurrentException(__PRETTY_FUNCTION__);

        /// Files can be re-created on:
        /// - module load/unload
        /// - suspend/resume cycle
        /// So file descriptors should be reopened.
        try
        {
            openSensorsChips();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    try
    {
        for (size_t i = 0, size = edac.size(); i < size; ++i)
        {
            /// NOTE maybe we need to take difference with previous values.
            /// But these metrics should be exceptionally rare, so it's ok to keep them accumulated.

            if (edac[i].first)
            {
                ReadBufferFromFilePRead & in = *edac[i].first;
                in.rewind();
                uint64_t errors = 0;
                readText(errors, in);
                new_values[fmt::format("EDAC{}_Correctable", i)] = { errors,
                    "The number of correctable ECC memory errors."
                    " A high number of this value indicates bad RAM which has to be immediately replaced,"
                    " because in presence of a high number of corrected errors, a number of silent errors may happen as well, leading to data corruption."
                    " Source: `/sys/devices/system/edac/mc/`" };
            }

            if (edac[i].second)
            {
                ReadBufferFromFilePRead & in = *edac[i].second;
                in.rewind();
                uint64_t errors = 0;
                readText(errors, in);
                new_values[fmt::format("EDAC{}_Uncorrectable", i)] = { errors,
                    "The number of uncorrectable ECC memory errors."
                    " A non-zero number of this value indicates bad RAM which has to be immediately replaced,"
                    " because it indicates potential data corruption."
                    " Source: `/sys/devices/system/edac/mc/`" };
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// EDAC files can be re-created on module load/unload
        try
        {
            openEDAC();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
#endif

    {
        auto get_metric_name_doc = [](const String & name) -> std::pair<const char *, const char *>
        {
            static std::map<String, std::pair<const char *, const char *>> metric_map =
            {
                {"tcp_port", {"TCPThreads", "Number of threads in the server of the TCP protocol (without TLS)."}},
                {"tcp_port_secure", {"TCPSecureThreads", "Number of threads in the server of the TCP protocol (with TLS)."}},
                {"http_port", {"HTTPThreads", "Number of threads in the server of the HTTP interface (without TLS)."}},
                {"https_port", {"HTTPSecureThreads", "Number of threads in the server of the HTTPS interface."}},
                {"interserver_http_port", {"InterserverThreads", "Number of threads in the server of the replicas communication protocol (without TLS)."}},
                {"interserver_https_port", {"InterserverSecureThreads", "Number of threads in the server of the replicas communication protocol (with TLS)."}},
                {"mysql_port", {"MySQLThreads", "Number of threads in the server of the MySQL compatibility protocol."}},
                {"postgresql_port", {"PostgreSQLThreads", "Number of threads in the server of the PostgreSQL compatibility protocol."}},
                {"grpc_port", {"GRPCThreads", "Number of threads in the server of the GRPC protocol."}},
                {"prometheus.port", {"PrometheusThreads", "Number of threads in the server of the Prometheus endpoint. Note: prometheus endpoints can be also used via the usual HTTP/HTTPs ports."}},
                {"keeper_server.tcp_port", {"KeeperTCPThreads", "Number of threads in the server of the Keeper TCP protocol (without TLS)."}},
                {"keeper_server.tcp_port_secure", {"KeeperTCPSecureThreads", "Number of threads in the server of the Keeper TCP protocol (with TLS)."}}
            };
            auto it = metric_map.find(name);
            if (it == metric_map.end())
                return { nullptr, nullptr };
            else
                return it->second;
        };

        const auto server_metrics = protocol_server_metrics_func();
        for (const auto & server_metric : server_metrics)
        {
            if (auto name_doc = get_metric_name_doc(server_metric.port_name); name_doc.first != nullptr)
                new_values[name_doc.first] = { server_metric.current_threads, name_doc.second };
        }
    }

    /// Add more metrics as you wish.

    updateImpl(new_values, update_time, current_time);

    new_values["AsynchronousMetricsCalculationTimeSpent"] = { watch.elapsedSeconds(), "Time in seconds spent for calculation of asynchronous metrics (this is the overhead of asynchronous metrics)." };

    logImpl(new_values);

    first_run = false;

    // Finally, update the current metrics.
    std::lock_guard lock(mutex);
    values = new_values;
}

}
