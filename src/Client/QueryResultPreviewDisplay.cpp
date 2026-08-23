#include <Client/QueryResultPreviewDisplay.h>

#include <Common/TerminalSize.h>
#include <Common/UTF8Helpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>

#include <boost/algorithm/string/split.hpp>
#include <fmt/format.h>

namespace DB
{

namespace
{
    constexpr std::string_view CLEAR_TO_END_OF_LINE = "\033[K";
    constexpr std::string_view CLEAR_TO_END_OF_SCREEN = "\033[0J";
    constexpr std::string_view HIDE_CURSOR = "\033[?25l";
    constexpr std::string_view SHOW_CURSOR = "\033[?25h";
    constexpr std::string_view MUTED_COLOR = "\033[90m";
    constexpr std::string_view RESET_COLOR = "\033[0m";

    std::string moveUpNLines(size_t n)
    {
        return fmt::format("\033[{}A", n);
    }
}

void QueryResultPreviewDisplay::setPreview(const Block & block, ContextPtr context)
{
    auto [terminal_width, terminal_height] = getTerminalSize(in_fd, err_fd);
    if (terminal_width == 0 || terminal_height < 8)
        return;

    auto format_settings = getFormatSettings(context);
    format_settings.is_writing_to_terminal = false;
    format_settings.pretty.color = 0;
    format_settings.pretty.glue_chunks = 0;
    format_settings.pretty.squash_consecutive_ms = 0;
    format_settings.pretty.fallback_to_vertical = false;
    format_settings.pretty.multiline_fields = false;
    format_settings.pretty.display_footer_column_names = 0;
    /// The preview must fit under the progress bar without scrolling the terminal.
    format_settings.pretty.max_rows = terminal_height - 7;
    format_settings.pretty.max_value_width = std::min<UInt64>(format_settings.pretty.max_value_width, terminal_width);

    WriteBufferFromOwnString out;
    auto output_format = FormatFactory::instance().getOutputFormat("PrettyCompactNoEscapes", out, block.cloneEmpty(), context, format_settings);
    output_format->write(block);
    output_format->finalize();

    std::vector<std::string> new_lines;
    boost::split(new_lines, out.str(), [](char c) { return c == '\n'; });
    while (!new_lines.empty() && new_lines.back().empty())
        new_lines.pop_back();

    /// Rendering a line wider than the terminal would wrap and break the cursor arithmetic.
    for (const auto & line : new_lines)
        if (UTF8::computeWidth(reinterpret_cast<const UInt8 *>(line.data()), line.size()) > terminal_width)
            return;

    if (new_lines.size() + 3 > terminal_height)
        return;

    std::lock_guard lock(mutex);
    lines = std::move(new_lines);
}

void QueryResultPreviewDisplay::writePreview(WriteBufferFromFileDescriptor & message, std::unique_lock<std::mutex> &)
{
    std::lock_guard lock(mutex);
    if (lines.empty())
        return;

    message << HIDE_CURSOR;

    for (const auto & line : lines)
        message << "\n" << MUTED_COLOR << line << RESET_COLOR << CLEAR_TO_END_OF_LINE;

    message << CLEAR_TO_END_OF_SCREEN << moveUpNLines(lines.size());
    message.next();

    painted_lines = lines.size();
}

void QueryResultPreviewDisplay::clearPreviewOutput(WriteBufferFromFileDescriptor & message, std::unique_lock<std::mutex> &)
{
    std::lock_guard lock(mutex);
    if (painted_lines == 0)
        return;

    /// The cursor sits on the anchor (progress bar) line; erase everything below it.
    message << "\r" << CLEAR_TO_END_OF_SCREEN << SHOW_CURSOR;
    message.next();

    painted_lines = 0;
}

void QueryResultPreviewDisplay::resetPreview()
{
    std::lock_guard lock(mutex);
    lines.clear();
    painted_lines = 0;
}

}
