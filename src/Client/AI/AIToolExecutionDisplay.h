#pragma once

#include <algorithm>
#include <atomic>
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
        : output_stream(output_stream_), use_colors(enable_colors), thinking_active(false)
    {
    }

    ~AIToolExecutionDisplay()
    {
        // Ensure thinking animation is stopped on destruction
        stopThinking();
    }

    void startThinking(const std::string & message = "thinking") const
    {
        // Stop any existing thinking animation
        stopThinking();

        thinking_active = true;
        thinking_thread = std::thread([this, message]()
        {
            int dot_count = 0;
            while (thinking_active)
            {
                // Clear the line and show the message with dots
                output_stream << "\r\033[K"; // Clear line

                if (use_colors)
                    output_stream << "\033[36m"; // Cyan
                output_stream << "🧠 " << message;

                // Show dots based on current count (cycles through 0, 1, 2, 3 dots)
                for (int i = 0; i < dot_count; ++i)
                {
                    output_stream << ".";
                }

                if (use_colors)
                    output_stream << resetColor();

                output_stream << std::flush;

                // Sleep before next update
                std::this_thread::sleep_for(std::chrono::milliseconds(400));

                // Cycle through 0-3 dots
                dot_count = (dot_count + 1) % 4;
            }

            // Clear the line when done
            output_stream << "\r\033[K" << std::flush;
        });
    }

    void stopThinking() const
    {
        if (thinking_active)
        {
            thinking_active = false;
            if (thinking_thread.joinable())
            {
                thinking_thread.join();
            }
        }
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
    mutable std::atomic<bool> thinking_active;
    mutable std::thread thinking_thread;
};

}
