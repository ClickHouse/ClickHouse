#pragma once

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <base/terminalColors.h>
#include <ostream>
#include <Common/StringUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

class AIToolExecutionDisplay
{
public:
    explicit AIToolExecutionDisplay(std::ostream & output_stream_, bool enable_colors = true)
        : output_stream(output_stream_), use_colors(enable_colors)
    {
    }

    void showThinking() const
    {
        if (use_colors)
            output_stream << "\033[36m"; // Cyan
        output_stream << "🧠 thinking";
        // Animated dots
        for (int i = 0; i < 3; ++i)
        {
            output_stream << "." << std::flush;
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }

        // Clear the line
        output_stream << "\r\033[K";

        if (use_colors)
            output_stream << resetColor();
    }

    void showToolCall(const std::string & tool_call_id, const std::string & function_name, const std::string & arguments = "") const
    {
        // Clear any previous line
        output_stream << "\r\033[K";

        // Tool call header with icon
        if (use_colors)
            output_stream << "\033[33m"; // Yellow
        output_stream << "🔧 ";
        if (use_colors)
            output_stream << "\033[1m"; // Bold
        output_stream << "Calling: " << function_name;
        if (use_colors)
            output_stream << "\033[0m" << "\033[33m"; // Reset bold, keep yellow
        output_stream << " [" << tool_call_id.substr(0, 8) << "...]";
        if (use_colors)
            output_stream << resetColor();
        output_stream << std::endl;

        // Show arguments if provided (indented)
        if (!arguments.empty() && arguments != "{}")
        {
            if (use_colors)
                output_stream << "\033[90m"; // Dark gray
            output_stream << "  └─ Args: " << arguments;
            if (use_colors)
                output_stream << resetColor();
            output_stream << std::endl;
        }
    }

    void showToolResult(const std::string & function_name, const std::string & result, bool success = true) const
    {
        // Result with icon
        if (use_colors)
            output_stream << (success ? "\033[32m" : "\033[31m"); // Green for success, Red for error
        output_stream << (success ? "✓" : "✗") << " ";
        if (use_colors)
            output_stream << "\033[1m"; // Bold
        output_stream << function_name << " completed";
        if (use_colors)
            output_stream << "\033[0m"; // Reset bold
        output_stream << std::endl;

        // Show result preview (truncated if too long)
        if (!result.empty())
        {
            std::string preview = result;
            // Replace newlines with spaces for preview
            std::replace(preview.begin(), preview.end(), '\n', ' ');

            // Truncate if too long
            const size_t max_preview_length = 80;
            if (preview.length() > max_preview_length)
            {
                preview = preview.substr(0, max_preview_length - 3) + "...";
            }

            if (use_colors)
                output_stream << "\033[90m"; // Dark gray
            output_stream << "  └─ " << preview;
            if (use_colors)
                output_stream << resetColor();
            output_stream << std::endl;
        }
    }

    void showProgress(const std::string & message) const
    {
        if (use_colors)
            output_stream << "\033[36m"; // Cyan
        output_stream << "• " << message;
        if (use_colors)
            output_stream << resetColor();
        output_stream << std::endl;
    }

    void showGeneratedQuery(const std::string & query, const std::string & description = "") const
    {
        // Clear any previous line
        output_stream << "\r\033[K";

        if (use_colors)
            output_stream << "\033[90m"; // Dark gray
        if (!description.empty())
        {
            output_stream << description << ":";
        }
        else
        {
            output_stream << "query:";
        }
        if (use_colors)
            output_stream << resetColor();
        output_stream << std::endl;

        if (!query.empty())
        {
            if (use_colors)
                output_stream << "\033[37m"; // Light gray

            output_stream << query << std::endl;

            if (use_colors)
                output_stream << resetColor();
        }
    }

    void showSeparator() const
    {
        if (use_colors)
            output_stream << "\033[90m"; // Dark gray
        output_stream << "─────────────────────────────────────────────────";
        if (use_colors)
            output_stream << resetColor();
        output_stream << std::endl;
    }

private:
    std::ostream & output_stream;
    bool use_colors;
};

}
