#pragma once

#include <Interpreters/ProfileEventsExt.h>
#include <base/types.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <map>
#include <mutex>
#include <ostream>
#include <string_view>
#include <unordered_map>
#include <unistd.h>

namespace DB
{

class WriteBufferFromFileDescriptor;
class Block;

class ProgressTable
{
public:
    explicit ProgressTable(std::ostream & output_stream_, int in_fd_ = STDIN_FILENO, int err_fd_ = STDERR_FILENO)
        : output_stream(output_stream_), in_fd(in_fd_), err_fd(err_fd_)
    {
    }

    /// Write progress table with metrics.
    void writeTable(WriteBufferFromFileDescriptor & message, bool show_table, bool toggle_enabled);
    void clearTableOutput(WriteBufferFromFileDescriptor & message);
    void writeFinalTable();

    /// Update the metric values. They can be updated from:
    /// onProfileEvents in clickhouse-client;
    void updateTable(const Block & block);

    /// Reset progress table values.
    void resetTable();

private:
    class MetricInfo
    {
    public:
        explicit MetricInfo(ProfileEvents::Type t);

        void updateValue(Int64 new_value, double new_time);
        double calculateProgress(double time_now) const;
        double getValue() const;
        bool isStale(double now) const;

    private:
        const ProfileEvents::Type type;

        struct Snapshot
        {
            Int64 value = 0;
            double time = 0;
        };

        /// The previous and current snapshots are used by `calculateProgress`.
        /// They contain information that is outdated by about a second.
        /// The new snapshot is used by `updateValue` and `getValue`.
        /// We don't use a new snapshot in `calculateProgress` because the time elapsed since
        /// the previous update may be very small, causing jitter.
        Snapshot prev_shapshot;
        Snapshot cur_shapshot;
        Snapshot new_snapshot;

        double update_time = 0.0;
    };

    class MetricInfoPerHost
    {
    public:
        using HostName = String;

        void updateHostValue(const HostName & host, ProfileEvents::Type type, Int64 new_value, double new_time);
        double getSummaryValue();
        double getSummaryProgress(double time_now);
        double getMaxProgress() const;
        bool isStale(double now) const;

    private:
        std::unordered_map<HostName, MetricInfo> host_to_metric;
        double max_progress = 0;
    };

    size_t tableSize() const;

    using MetricName = String;

    /// The server periodically sends Block with profile events.
    /// This information is stored here.
    std::map<MetricName, MetricInfoPerHost> metrics;

    /// It is possible concurrent access to the metrics.
    std::mutex mutex;

    /// Track query execution time on client.
    Stopwatch watch;

    bool written_first_block = false;

    size_t column_event_name_width = 20;

    static constexpr std::string_view COLUMN_EVENT_NAME = "Event name";
    static constexpr std::string_view COLUMN_VALUE = "Value";
    static constexpr std::string_view COLUMN_PROGRESS = "Progress";
    static constexpr std::string_view COLUMN_DOCUMENTATION_NAME = "Documentation";
    static constexpr size_t COLUMN_VALUE_WIDTH = 20;
    static constexpr size_t COLUMN_PROGRESS_WIDTH = 20;
    static constexpr size_t COLUMN_DOCUMENTATION_WIDTH = 100;

    std::ostream & output_stream;
    int in_fd;
    int err_fd;
};

}
