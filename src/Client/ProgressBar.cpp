#include "ProgressBar.h"
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Common/TerminalSize.h>
#include <Common/UnicodeBar.h>
#include <Databases/DatabaseMemory.h>


/// http://en.wikipedia.org/wiki/ANSI_escape_code
#define CLEAR_TO_END_OF_LINE "\033[K"


namespace DB {
bool ProgressBar::getNeedRenderProgress() const
{
    return need_render_progress;
}

bool ProgressBar::getShowProgressBar() const
{
    return show_progress_bar;
}

size_t ProgressBar::getWrittenProgressChars() const
{
    return written_progress_chars;
}

bool ProgressBar::isWrittenFirstBlock() const
{
    return written_first_block;
}

void ProgressBar::setNeedRenderProgress(bool needRenderProgress)
{
    need_render_progress = needRenderProgress;
}

void ProgressBar::setShowProgressBar(bool showProgressBar)
{
    show_progress_bar = showProgressBar;
}

void ProgressBar::setWrittenProgressChars(size_t writtenProgressChars)
{
    written_progress_chars = writtenProgressChars;
}

void ProgressBar::setWrittenFirstBlock(bool writtenFirstBlock)
{
    written_first_block = writtenFirstBlock;
}

void ProgressBar::updateProgress(Progress & progress, const Progress &value)
{
    progress.incrementPiecewiseAtomically(value);
}

void ProgressBar::writeProgress(const Progress & progress, const Stopwatch & watch) {
    if (!need_render_progress)
        return;

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

    message << indicator << " Progress: ";

    message
        << formatReadableQuantity(progress.read_rows) << " rows, "
        << formatReadableSizeWithDecimalSuffix(progress.read_bytes);

    size_t elapsed_ns = watch.elapsed();
    if (elapsed_ns)
        message << " ("
                << formatReadableQuantity(progress.read_rows * 1000000000.0 / elapsed_ns) << " rows/s., "
                << formatReadableSizeWithDecimalSuffix(progress.read_bytes * 1000000000.0 / elapsed_ns) << "/s.) ";
    else
        message << ". ";

    written_progress_chars = message.count() - prefix_size - (strlen(indicator) - 2); /// Don't count invisible output (escape sequences).

    /// If the approximate number of rows to process is known, we can display a progress bar and percentage.
    if (progress.total_rows_to_read > 0)
    {
        size_t total_rows_corrected = std::max(progress.read_rows, progress.total_rows_to_read);

        /// To avoid flicker, display progress bar only if .5 seconds have passed since query execution start
        ///  and the query is less than halfway done.

        if (elapsed_ns > 500000000)
        {
            /// Trigger to start displaying progress bar. If query is mostly done, don't display it.
            if (progress.read_rows * 2 < total_rows_corrected)
                show_progress_bar = true;

            if (show_progress_bar)
            {
                ssize_t width_of_progress_bar = static_cast<ssize_t>(terminal_width) - written_progress_chars - strlen(" 99%");
                if (width_of_progress_bar > 0)
                {
                    std::string bar
                        = UnicodeBar::render(UnicodeBar::getWidth(progress.read_rows, 0, total_rows_corrected, width_of_progress_bar));
                    message << "\033[0;32m" << bar << "\033[0m";
                    if (width_of_progress_bar > static_cast<ssize_t>(bar.size() / UNICODE_BAR_CHAR_SIZE))
                        message << std::string(width_of_progress_bar - bar.size() / UNICODE_BAR_CHAR_SIZE, ' ');
                }
            }
        }

        /// Underestimate percentage a bit to avoid displaying 100%.
        message << ' ' << (99 * progress.read_rows / total_rows_corrected) << '%';
    }

    message << CLEAR_TO_END_OF_LINE;
    ++increment;

    message.next();
}


}