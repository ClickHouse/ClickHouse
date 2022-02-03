#include "ProgressIndication.h"
#include <algorithm>
#include <cstddef>
#include <numeric>
#include <cmath>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <base/types.h>
#include "Common/formatReadable.h"
#include <Common/TerminalSize.h>
#include <Common/UnicodeBar.h>
#include "IO/WriteBufferFromString.h"
#include <Databases/DatabaseMemory.h>


namespace
{
    constexpr UInt64 ALL_THREADS = 0;

    UInt64 calculateCoresNumber(DB::ThreadIdToTimeMap times, UInt64 elapsed)
    {
        auto accumulated = std::accumulate(times.begin(), times.end(), 0,
        [](Int64 acc, const auto & elem)
        {
            if (elem.first == ALL_THREADS)
                return acc;
            return acc + elem.second.time();
        });
        return (static_cast<UInt64>(accumulated) + elapsed - 1) / elapsed;
    }
}

namespace DB
{

bool ProgressIndication::updateProgress(const Progress & value)
{
    return progress.incrementPiecewiseAtomically(value);
}

void ProgressIndication::clearProgressOutput()
{
    if (written_progress_chars)
    {
        written_progress_chars = 0;
        std::cerr << "\r" CLEAR_TO_END_OF_LINE;
    }
}

void ProgressIndication::resetProgress()
{
    watch.restart();
    progress.reset();
    show_progress_bar = false;
    written_progress_chars = 0;
    write_progress_on_update = false;
    host_active_cores.clear();
    thread_data.clear();
}

void ProgressIndication::setFileProgressCallback(ContextMutablePtr context, bool write_progress_on_update_)
{
    write_progress_on_update = write_progress_on_update_;
    context->setFileProgressCallback([&](const FileProgress & file_progress)
    {
        progress.incrementPiecewiseAtomically(Progress(file_progress));

        if (write_progress_on_update)
            writeProgress();
    });
}

void ProgressIndication::addThreadIdToList(String const & host, UInt64 thread_id)
{
    auto & thread_to_times = thread_data[host];
    if (thread_to_times.contains(thread_id))
        return;
    thread_to_times[thread_id] = {};
}

void ProgressIndication::updateThreadEventData(HostToThreadTimesMap & new_thread_data, UInt64 elapsed_time)
{
    for (auto & new_host_map : new_thread_data)
    {
        auto new_cores = calculateCoresNumber(new_host_map.second, elapsed_time);
        host_active_cores[new_host_map.first] = new_cores;
        thread_data[new_host_map.first] = std::move(new_host_map.second);
    }
}

size_t ProgressIndication::getUsedThreadsCount() const
{
    return std::accumulate(thread_data.cbegin(), thread_data.cend(), 0,
        [] (size_t acc, auto const & threads)
        {
            return acc + threads.second.size();
        });
}

UInt64 ProgressIndication::getApproximateCoresNumber() const
{
    return std::accumulate(host_active_cores.cbegin(), host_active_cores.cend(), 0,
        [](UInt64 acc, auto const & elem)
        {
            return acc + elem.second;
        });
}

ProgressIndication::MemoryUsage ProgressIndication::getMemoryUsage() const
{
    return std::accumulate(thread_data.cbegin(), thread_data.cend(), MemoryUsage{},
        [](MemoryUsage const & acc, auto const & host_data)
        {
            UInt64 host_usage = 0;
            // In ProfileEvents packets thread id 0 specifies common profiling information
            // for all threads executing current query on specific host. So instead of summing per thread
            // memory consumption it's enough to look for data with thread id 0.
            if (auto it = host_data.second.find(ALL_THREADS); it != host_data.second.end())
                host_usage = it->second.memory_usage;
            return MemoryUsage{.total = acc.total + host_usage, .max = std::max(acc.max, host_usage)};
        });
}

void ProgressIndication::writeFinalProgress()
{
    if (progress.read_rows < 1000)
        return;

    std::cout << "Processed " << formatReadableQuantity(progress.read_rows) << " rows, "
                << formatReadableSizeWithDecimalSuffix(progress.read_bytes);

    size_t elapsed_ns = watch.elapsed();
    if (elapsed_ns)
        std::cout << " (" << formatReadableQuantity(progress.read_rows * 1000000000.0 / elapsed_ns) << " rows/s., "
                    << formatReadableSizeWithDecimalSuffix(progress.read_bytes * 1000000000.0 / elapsed_ns) << "/s.)";
    else
        std::cout << ". ";
}

void ProgressIndication::writeProgress()
{
    /// Output all progress bar commands to stderr at once to avoid flicker.
    WriteBufferFromFileDescriptor message(STDERR_FILENO, 1024);

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

    size_t terminal_width = getTerminalWidth();

    if (!written_progress_chars)
    {
        /// If the current line is not empty, the progress must be output on the next line.
        /// The trick is found here: https://www.vidarholen.net/contents/blog/?p=878
        message << std::string(terminal_width, ' ');
    }
    message << '\r';

    size_t prefix_size = message.count();
    size_t read_bytes = progress.read_raw_bytes ? progress.read_raw_bytes : progress.read_bytes;

    message << indicator << " Progress: ";
    message
        << formatReadableQuantity(progress.read_rows) << " rows, "
        << formatReadableSizeWithDecimalSuffix(read_bytes);

    auto elapsed_ns = watch.elapsed();
    if (elapsed_ns)
        message << " ("
                << formatReadableQuantity(progress.read_rows * 1000000000.0 / elapsed_ns) << " rows/s., "
                << formatReadableSizeWithDecimalSuffix(read_bytes * 1000000000.0 / elapsed_ns) << "/s.) ";
    else
        message << ". ";

    written_progress_chars = message.count() - prefix_size - (strlen(indicator) - 2); /// Don't count invisible output (escape sequences).

    // If approximate cores number is known, display it.
    auto cores_number = getApproximateCoresNumber();
    std::string profiling_msg;
    if (cores_number != 0 && print_hardware_utilization)
    {
        WriteBufferFromOwnString profiling_msg_builder;
        // Calculated cores number may be not accurate
        // so it's better to print min(threads, cores).
        UInt64 threads_number = getUsedThreadsCount();
        profiling_msg_builder << " Running " << threads_number << " threads on "
            << std::min(cores_number, threads_number) << " cores";

        auto [memory_usage, max_host_usage] = getMemoryUsage();
        if (memory_usage != 0)
            profiling_msg_builder << " with " << formatReadableSizeWithDecimalSuffix(memory_usage) << " RAM used";
        if (thread_data.size() > 1 && max_host_usage)
            profiling_msg_builder << " total (per host max: " << formatReadableSizeWithDecimalSuffix(max_host_usage) << ")";
        profiling_msg_builder << ".";
        profiling_msg = profiling_msg_builder.str();
    }

    /// If the approximate number of rows to process is known, we can display a progress bar and percentage.
    if (progress.total_rows_to_read || progress.total_raw_bytes_to_read)
    {
        size_t current_count, max_count;
        if (progress.total_rows_to_read)
        {
            current_count = progress.read_rows;
            max_count = std::max(progress.read_rows, progress.total_rows_to_read);
        }
        else
        {
            current_count = progress.read_raw_bytes;
            max_count = std::max(progress.read_raw_bytes, progress.total_raw_bytes_to_read);
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
                ssize_t width_of_progress_bar = static_cast<ssize_t>(terminal_width) - written_progress_chars - strlen(" 99%") - profiling_msg.length();
                if (width_of_progress_bar > 0)
                {
                    std::string bar
                        = UnicodeBar::render(UnicodeBar::getWidth(current_count, 0, max_count, width_of_progress_bar));
                    message << "\033[0;32m" << bar << "\033[0m";
                    if (width_of_progress_bar > static_cast<ssize_t>(bar.size() / UNICODE_BAR_CHAR_SIZE))
                        message << std::string(width_of_progress_bar - bar.size() / UNICODE_BAR_CHAR_SIZE, ' ');
                }
            }
        }

        /// Underestimate percentage a bit to avoid displaying 100%.
        message << ' ' << (99 * current_count / max_count) << '%';
    }

    message << profiling_msg;
    message << CLEAR_TO_END_OF_LINE;
    ++increment;

    message.next();
}

}
