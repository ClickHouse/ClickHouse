#include <Common/ProgressIndication.h>
#include <algorithm>
#include <cstddef>
#include <mutex>
#include <numeric>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <base/types.h>
#include <Common/formatReadable.h>
#include <Common/TerminalSize.h>
#include <Common/UnicodeBar.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>

/// http://en.wikipedia.org/wiki/ANSI_escape_code
#define CLEAR_TO_END_OF_LINE "\033[K"


namespace DB
{

UInt64 ProgressIndication::getElapsedNanoseconds() const
{
    /// New server versions send server-side elapsed time, which is preferred for calculations.
    UInt64 server_elapsed_ns = progress.elapsed_ns.load(std::memory_order_relaxed);
    return server_elapsed_ns ? server_elapsed_ns : watch.elapsed();
}

bool ProgressIndication::updateProgress(const Progress & value)
{
    return progress.incrementPiecewiseAtomically(value);
}

void ProgressIndication::resetProgress()
{
    {
        std::lock_guard lock(progress_mutex);
        progress.reset();
        show_progress_bar = false;
        written_progress_chars = 0;
        bar_segments.clear();
        bar_segments_in_rows = false;
        write_progress_on_update = false;
    }
    {
        std::lock_guard lock(profile_events_mutex);
        watch.restart();
        cpu_usage_meter.reset(static_cast<double>(getElapsedNanoseconds()));
        waited_meter.reset(static_cast<double>(getElapsedNanoseconds()));
        hosts_data.clear();
    }
}

void ProgressIndication::setFileProgressCallback(ContextMutablePtr context, WriteBufferFromFileDescriptor & message, std::mutex & message_mutex)
{
    context->setFileProgressCallback([&](const FileProgress & file_progress)
    {
        progress.incrementPiecewiseAtomically(Progress(file_progress));
        std::unique_lock message_lock(message_mutex);
        writeProgress(message, message_lock);
    });
}

void ProgressIndication::updateThreadEventData(HostToTimesMap & new_hosts_data)
{
    std::lock_guard lock(profile_events_mutex);

    constexpr UInt64 us_to_ns = 1000;

    UInt64 total_cpu_ns = 0;
    UInt64 total_waited_ns = 0;
    for (auto & new_host : new_hosts_data)
    {
        total_cpu_ns += us_to_ns * new_host.second.time();
        total_waited_ns += us_to_ns * new_host.second.waited_us;
        hosts_data[new_host.first] = new_host.second;
    }
    double now = static_cast<double>(getElapsedNanoseconds());
    cpu_usage_meter.add(now, static_cast<double>(total_cpu_ns));
    waited_meter.add(now, static_cast<double>(total_waited_ns));
}

double ProgressIndication::getCPUUsage()
{
    std::lock_guard lock(profile_events_mutex);
    return cpu_usage_meter.rate(static_cast<double>(getElapsedNanoseconds()));
}

double ProgressIndication::getWaitedUsage()
{
    std::lock_guard lock(profile_events_mutex);
    return waited_meter.rate(static_cast<double>(getElapsedNanoseconds()));
}

ProgressIndication::MemoryUsage ProgressIndication::getMemoryUsage() const
{
    std::lock_guard lock(profile_events_mutex);

    return std::accumulate(hosts_data.cbegin(), hosts_data.cend(), MemoryUsage{},
        [](MemoryUsage const & acc, auto const & host_data)
        {
            UInt64 host_usage = host_data.second.memory_usage;
            return MemoryUsage{.total = acc.total + host_usage, .max = std::max(acc.max, host_usage), .peak = std::max(acc.peak, host_data.second.peak_memory_usage)};
        });
}

ProgressIndication::TempDataOnDiskUsage ProgressIndication::getTempDataOnDiskUsage() const
{
    std::lock_guard lock(profile_events_mutex);

    return std::accumulate(hosts_data.cbegin(), hosts_data.cend(), TempDataOnDiskUsage{},
        [](TempDataOnDiskUsage const & acc, auto const & host_data)
        {
            UInt64 host_usage = host_data.second.temp_data_on_disk_usage;
            return TempDataOnDiskUsage{.total = acc.total + host_usage, .max = std::max(acc.max, host_usage)};
        });
}

void ProgressIndication::writeFinalProgress()
{
    std::lock_guard lock(progress_mutex);

    if (progress.read_rows < 1000)
        return;

    output_stream << "Processed " << formatReadableQuantity(progress.read_rows.load()) << " rows, "
                  << formatReadableSizeWithDecimalSuffix(progress.read_bytes.load());

    UInt64 elapsed_ns = getElapsedNanoseconds();
    if (elapsed_ns)
        output_stream << " (" << formatReadableQuantity(static_cast<double>(progress.read_rows.load()) * 1000000000.0 / static_cast<double>(elapsed_ns)) << " rows/s., "
                    << formatReadableSizeWithDecimalSuffix(static_cast<double>(progress.read_bytes.load()) * 1000000000.0 / static_cast<double>(elapsed_ns)) << "/s.)";
    else
        output_stream << ". ";

    auto peak_memory_usage = getMemoryUsage().peak;
    if (peak_memory_usage >= 0)
        output_stream << "\nPeak memory usage: " << formatReadableSizeWithBinarySuffix(peak_memory_usage) << ".";
}

void ProgressIndication::writeProgress(WriteBufferFromFileDescriptor & message, std::unique_lock<std::mutex> &)
{
    std::lock_guard lock(progress_mutex);

    static size_t increment = 0;
    static const char * indicators[8] = {
        "\033[1;30m→\033[0m",
        "\033[1;31m↘\033[0m",
        "\033[1;32m↓\033[0m",
        "\033[1;33m↙\033[0m",
        "\033[1;34m←\033[0m",
        "\033[1;35m↖\033[0m",
        "\033[1;36m↑\033[0m",
        "\033[1m↗\033[0m",
    };

    const char * indicator = indicators[increment % 8];

    auto [terminal_width, terminal_height] = getTerminalSize(in_fd, err_fd);

    if (!written_progress_chars)
    {
        /// If the current line is not empty, the progress must be output on the next line.
        /// The trick is found here: https://www.vidarholen.net/contents/blog/?p=878
        message << std::string(terminal_width, ' ');
    }
    message << '\r';

    size_t prefix_size = message.count();

    message << indicator << " Progress: ";
    message
        << formatReadableQuantity(progress.read_rows.load()) << " rows, "
        << formatReadableSizeWithDecimalSuffix(progress.read_bytes.load());

    UInt64 elapsed_ns = getElapsedNanoseconds();
    if (elapsed_ns)
        message << " ("
                << formatReadableQuantity(static_cast<double>(progress.read_rows.load()) * 1000000000.0 / static_cast<double>(elapsed_ns)) << " rows/s., "
                << formatReadableSizeWithDecimalSuffix(static_cast<double>(progress.read_bytes.load()) * 1000000000.0 / static_cast<double>(elapsed_ns)) << "/s.) ";
    else
        message << ". ";

    written_progress_chars = message.count() - prefix_size - (strlen(indicator) - 2); /// Don't count invisible output (escape sequences).

    /// Display resource usage if possible.
    std::string profiling_msg;

    /// We don't want -0. that can appear due to rounding errors, and a query that is not waiting
    /// at all must not count as stalled just because its CPU usage rounded to a negative value.
    double cpu_usage = std::max(getCPUUsage(), 0.);
    double waited = std::max(getWaitedUsage(), 0.);
    auto [memory_usage, max_host_usage, peak_usage] = getMemoryUsage();
    auto [temp_data_on_disk_usage, max_host_temp_data_on_disk_usage] = getTempDataOnDiskUsage();

    /// Mostly waiting instead of working: yellow instead of green.
    bool stalled = waited > cpu_usage;

    if (cpu_usage > 0 || waited > 0 || memory_usage > 0 || temp_data_on_disk_usage > 0)
    {
        WriteBufferFromOwnString profiling_msg_builder;

        profiling_msg_builder << "(" << fmt::format("{:.1f}", cpu_usage) << " CPU";

        if (waited > 0)
            profiling_msg_builder << ", " << fmt::format("{:.1f}", waited) << " waited";
        if (memory_usage > 0)
            profiling_msg_builder << ", " << formatReadableSizeWithDecimalSuffix(memory_usage) << " RAM";
        if (max_host_usage < memory_usage)
            profiling_msg_builder << ", " << formatReadableSizeWithDecimalSuffix(max_host_usage) << " max/host";
        if (temp_data_on_disk_usage > 0)
            profiling_msg_builder << ", " << formatReadableSizeWithDecimalSuffix(temp_data_on_disk_usage) << " disk";
        if (max_host_temp_data_on_disk_usage < temp_data_on_disk_usage)
            profiling_msg_builder << ", " << formatReadableSizeWithDecimalSuffix(max_host_temp_data_on_disk_usage) << " max/host";

        profiling_msg_builder << ")";
        profiling_msg = profiling_msg_builder.str();
    }

    int64_t remaining_space = static_cast<int64_t>(terminal_width) - written_progress_chars;

    /// If the approximate number of rows to process is known, we can display a progress bar and percentage.
    if (progress.total_rows_to_read || progress.total_bytes_to_read)
    {
        size_t current_count = 0;
        size_t max_count = 0;
        bool count_in_rows = progress.total_rows_to_read != 0;
        if (progress.total_rows_to_read)
        {
            current_count = progress.read_rows;
            max_count = std::max(progress.read_rows, progress.total_rows_to_read);
        }
        else
        {
            current_count = progress.read_bytes;
            max_count = std::max(progress.read_bytes, progress.total_bytes_to_read);
        }

        /// To avoid flicker, display progress bar only if .5 seconds have passed since query execution start
        ///  and the query is less than halfway done.

        /// Trigger to start displaying progress bar. If query is mostly done, don't display it.
        if (elapsed_ns > 500000000 && current_count * 2 < max_count)
            show_progress_bar = true;

        /// The history is recorded from the first repaint on, even while the bar is not shown: the
        /// bar is hidden while the query is past 50% of the total known so far, and the total can
        /// still grow (a `MergeTree` read adds it part by part, a JOIN adds the probe side after
        /// the build side), which shows the bar later and colors the cells of the interval that
        /// was hidden. The history is compacted below, so recording it does not let it grow with
        /// the duration of the query.

        /// The counts are in rows while the total number of rows is known, and in bytes otherwise,
        /// and a query can switch from the second to the first: a source that reports only the size
        /// of a file (`StorageFile`, `StorageURL`, object storage) can be read before a source that
        /// adds a total number of rows. A count recorded in bytes means nothing once the counts are
        /// in rows, so the history of the previous carrier is dropped and recorded anew.
        if (bar_segments_in_rows != count_in_rows)
        {
            bar_segments.clear();
            bar_segments_in_rows = count_in_rows;
        }

        /// The first segment always covers the bar from its first cell, because `colored_bar`
        /// treats each stored count as the first cell of its segment. The progress bar appears
        /// only after some progress has been made, so seeding it with `current_count` would
        /// drop the already-filled prefix until the stalled state flips for the first time.
        if (bar_segments.empty())
            bar_segments.emplace_back(0, stalled);
        else if (bar_segments.back().second != stalled)
        {
            if (bar_segments.back().first != current_count)
                bar_segments.emplace_back(current_count, stalled);
            else if (bar_segments.size() > 1)
                /// No progress since the last flip: the last segment is empty, and the state
                /// flipped back to the one of the segment before it, which simply continues.
                bar_segments.pop_back();
            else
                bar_segments.back().second = stalled;
        }

        /// The state can flip on every progress update, so the history has to be compacted, or it
        /// would grow with the duration of the query and make every repaint slower. It is compacted
        /// at a fixed resolution, which is finer than any terminal, rather than at the current width
        /// of the bar: the stored counts stay independent of the terminal, so a repaint while the
        /// terminal is temporarily narrow (or the bar is hidden by the annotation) does not discard
        /// transitions that are visible again once it is widened. Transitions that fall into the
        /// same virtual cell cannot be told apart at that resolution: the cell keeps the count where
        /// it began and takes the later state, and neighbours of the same state are merged. The
        /// total may still grow and shift older transitions into one cell: the next repaint
        /// collapses them the same way.
        auto virtual_cell_of = [&](UInt64 count)
        {
            return static_cast<size_t>(UnicodeBar::getWidth(static_cast<double>(count), 0, static_cast<double>(max_count), static_cast<double>(bar_history_resolution)));
        };

        size_t kept = 0;
        for (const auto & segment : bar_segments)
        {
            auto to_keep = segment;
            if (kept > 0 && virtual_cell_of(bar_segments[kept - 1].first) == virtual_cell_of(to_keep.first))
            {
                to_keep.first = bar_segments[kept - 1].first;
                --kept;
            }
            if (kept > 0 && bar_segments[kept - 1].second == to_keep.second)
                continue;
            bar_segments[kept++] = to_keep;
        }
        bar_segments.resize(kept);

        if (elapsed_ns > 500000000)
        {
            if (show_progress_bar)
            {
                /// We will display profiling info only if there is enough space for it.
                int64_t width_of_progress_bar = remaining_space - strlen(" 99%");

                /// We need at least twice the space, because it will be displayed either
                /// at right after progress bar or at left on top of the progress bar.
                if (width_of_progress_bar <= 1 + 2 * static_cast<int64_t>(profiling_msg.size()))
                    profiling_msg.clear();

                /// Each cell is colored by the state at the time that progress was made. Segments that
                /// begin in the same cell of this (coarser) bar are not told apart: `colored_bar` skips
                /// the empty ranges, so the cell takes the state of the last segment beginning in it.
                auto cell_of = [&](UInt64 count)
                {
                    double width = UnicodeBar::getWidth(static_cast<double>(count), 0, static_cast<double>(max_count), static_cast<double>(std::max<int64_t>(width_of_progress_bar, 0)));
                    return static_cast<size_t>(width);
                };

                if (width_of_progress_bar > 0)
                {
                    double bar_width = UnicodeBar::getWidth(static_cast<double>(current_count), 0, static_cast<double>(max_count), static_cast<double>(width_of_progress_bar));
                    std::string bar = UnicodeBar::render(bar_width);
                    size_t bar_width_in_terminal = bar.size() / UNICODE_BAR_CHAR_SIZE;

                    auto colored_bar = [&](size_t from_cell)
                    {
                        WriteBufferFromOwnString out;
                        for (size_t i = 0; i < bar_segments.size(); ++i)
                        {
                            size_t begin = std::max(from_cell, std::min(bar_width_in_terminal, cell_of(bar_segments[i].first)));
                            size_t end = i + 1 < bar_segments.size() ? std::min(bar_width_in_terminal, cell_of(bar_segments[i + 1].first)) : bar_width_in_terminal;
                            if (begin < end)
                                out << (bar_segments[i].second ? "\033[0;33m" : "\033[0;32m")
                                    << bar.substr(begin * UNICODE_BAR_CHAR_SIZE, (end - begin) * UNICODE_BAR_CHAR_SIZE) << "\033[0m";
                        }
                        return out.str();
                    };

                    if (profiling_msg.empty())
                    {
                        message << colored_bar(0)
                            << std::string(width_of_progress_bar - bar_width_in_terminal, ' ');
                    }
                    else
                    {
                        bool render_profiling_msg_at_left = current_count * 2 >= max_count;

                        if (render_profiling_msg_at_left)
                        {
                            /// Render profiling_msg at left on top of the progress bar. The annotation
                            /// covers the first cells of the bar, so its background follows the same
                            /// history as the cells it hides, instead of the current state only: a
                            /// prefix that was throttled stays yellow under the text, too.
                            auto colored_overlay = [&]()
                            {
                                WriteBufferFromOwnString out;
                                size_t overlay_cells = profiling_msg.size();
                                for (size_t i = 0; i < bar_segments.size(); ++i)
                                {
                                    size_t begin = std::min(overlay_cells, cell_of(bar_segments[i].first));
                                    size_t end = i + 1 < bar_segments.size() ? std::min(overlay_cells, cell_of(bar_segments[i + 1].first)) : overlay_cells;
                                    if (begin < end)
                                        out << (bar_segments[i].second ? "\033[30;43m" : "\033[30;42m")
                                            << profiling_msg.substr(begin, end - begin) << "\033[0m";
                                }
                                return out.str();
                            };

                            message << colored_overlay()
                                << colored_bar(profiling_msg.size())
                                << std::string(width_of_progress_bar - bar_width_in_terminal, ' ');
                        }
                        else
                        {
                            /// Render profiling_msg at right after the progress bar.

                            message << colored_bar(0)
                                << std::string(width_of_progress_bar - bar_width_in_terminal - profiling_msg.size(), ' ')
                                << "\033[2m" << profiling_msg << "\033[0m";
                        }
                    }
                }
            }
        }

        /// Underestimate percentage a bit to avoid displaying 100%.
        message << ' ' << (99 * current_count / max_count) << '%';
    }
    else
    {
        /// We can still display profiling info.
        if (remaining_space >= static_cast<int64_t>(profiling_msg.size()))
        {
            if (remaining_space > static_cast<int64_t>(profiling_msg.size()))
                message << std::string(remaining_space - profiling_msg.size(), ' ');
            message << "\033[2m" << profiling_msg << "\033[0m";
        }
    }

    message << CLEAR_TO_END_OF_LINE;
    ++increment;

    message.next();
}

void ProgressIndication::clearProgressOutput(WriteBufferFromFileDescriptor & message, std::unique_lock<std::mutex> &)
{
    std::lock_guard lock(progress_mutex);

    if (written_progress_chars)
    {
        written_progress_chars = 0;
        message << "\r" CLEAR_TO_END_OF_LINE;
        message.next();
    }
}

}
