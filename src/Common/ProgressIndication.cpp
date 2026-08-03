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
        write_progress_on_update = false;
    }
    {
        std::lock_guard lock(profile_events_mutex);
        watch.restart();
        cpu_usage_meter.reset(getElapsedNanoseconds());
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
    for (auto & new_host : new_hosts_data)
    {
        total_cpu_ns += us_to_ns * new_host.second.time();
        hosts_data[new_host.first] = new_host.second;
    }
    cpu_usage_meter.add(getElapsedNanoseconds(), total_cpu_ns);
}

double ProgressIndication::getCPUUsage()
{
    std::lock_guard lock(profile_events_mutex);
    return cpu_usage_meter.rate(getElapsedNanoseconds());
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

void ProgressIndication::writeFinalProgress()
{
    std::lock_guard lock(progress_mutex);

    if (progress.read_rows < 1000)
        return;

    output_stream << "Processed " << formatReadableQuantity(progress.read_rows) << " rows, "
                << formatReadableSizeWithDecimalSuffix(progress.read_bytes);

    UInt64 elapsed_ns = getElapsedNanoseconds();
    if (elapsed_ns)
        output_stream << " (" << formatReadableQuantity(progress.read_rows * 1000000000.0 / elapsed_ns) << " rows/s., "
                    << formatReadableSizeWithDecimalSuffix(progress.read_bytes * 1000000000.0 / elapsed_ns) << "/s.)";
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
        << formatReadableQuantity(progress.read_rows) << " rows, "
        << formatReadableSizeWithDecimalSuffix(progress.read_bytes);

    UInt64 elapsed_ns = getElapsedNanoseconds();
    if (elapsed_ns)
        message << " ("
                << formatReadableQuantity(progress.read_rows * 1000000000.0 / elapsed_ns) << " rows/s., "
                << formatReadableSizeWithDecimalSuffix(progress.read_bytes * 1000000000.0 / elapsed_ns) << "/s.) ";
    else
        message << ". ";

    written_progress_chars = message.count() - prefix_size - (strlen(indicator) - 2); /// Don't count invisible output (escape sequences).

    /// Display resource usage if possible.
    std::string profiling_msg;

    double cpu_usage = getCPUUsage();
    auto [memory_usage, max_host_usage, peak_usage] = getMemoryUsage();

    if (cpu_usage > 0 || memory_usage > 0)
    {
        WriteBufferFromOwnString profiling_msg_builder;

        /// We don't want -0. that can appear due to rounding errors.
        cpu_usage = std::max(cpu_usage, 0.);

        profiling_msg_builder << "(" << fmt::format("{:.1f}", cpu_usage) << " CPU";

        if (memory_usage > 0)
            profiling_msg_builder << ", " << formatReadableSizeWithDecimalSuffix(memory_usage) << " RAM";
        if (max_host_usage < memory_usage)
            profiling_msg_builder << ", " << formatReadableSizeWithDecimalSuffix(max_host_usage) << " max/host";

        profiling_msg_builder << ")";
        profiling_msg = profiling_msg_builder.str();
    }

    int64_t remaining_space = static_cast<int64_t>(terminal_width) - written_progress_chars;

    /// If the approximate number of rows to process is known, we can display a progress bar and percentage.
    if (progress.total_rows_to_read || progress.total_bytes_to_read)
    {
        size_t current_count;
        size_t max_count;
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

        if (elapsed_ns > 500000000)
        {
            /// Trigger to start displaying progress bar. If query is mostly done, don't display it.
            if (current_count * 2 < max_count)
                show_progress_bar = true;

            if (show_progress_bar)
            {
                /// We will display profiling info only if there is enough space for it.
                int64_t width_of_progress_bar = remaining_space - strlen(" 99%");

                /// We need at least twice the space, because it will be displayed either
                /// at right after progress bar or at left on top of the progress bar.
                if (width_of_progress_bar <= 1 + 2 * static_cast<int64_t>(profiling_msg.size()))
                    profiling_msg.clear();

                if (width_of_progress_bar > 0)
                {
                    double bar_width = UnicodeBar::getWidth(current_count, 0, max_count, width_of_progress_bar);
                    std::string bar = UnicodeBar::render(bar_width);
                    size_t bar_width_in_terminal = bar.size() / UNICODE_BAR_CHAR_SIZE;

                    if (profiling_msg.empty())
                    {
                        message << "\033[0;32m" << bar << "\033[0m"
                            << std::string(width_of_progress_bar - bar_width_in_terminal, ' ');
                    }
                    else
                    {
                        bool render_profiling_msg_at_left = current_count * 2 >= max_count;

                        if (render_profiling_msg_at_left)
                        {
                            /// Render profiling_msg at left on top of the progress bar.

                            message << "\033[30;42m" << profiling_msg << "\033[0m"
                                << "\033[0;32m" << bar.substr(profiling_msg.size() * UNICODE_BAR_CHAR_SIZE) << "\033[0m"
                                << std::string(width_of_progress_bar - bar_width_in_terminal, ' ');
                        }
                        else
                        {
                            /// Render profiling_msg at right after the progress bar.

                            message << "\033[0;32m" << bar << "\033[0m"
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
