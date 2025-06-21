#pragma once

#include <string>
#include <iostream>
#include <chrono>
#include <thread>
#include <algorithm>
#include <base/terminalColors.h>

namespace DB
{

class AIToolExecutionDisplay
{
public:
    explicit AIToolExecutionDisplay(bool enable_colors = true) 
        : use_colors(enable_colors)
    {
    }

    void showThinking() const
    {
        if (use_colors)
            std::cerr << "\033[36m"; // Cyan
        std::cerr << "🧠 thinking";
        // Animated dots
        for (int i = 0; i < 3; ++i)
        {
            std::cerr << "." << std::flush;
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
        
        // Clear the line
        std::cerr << "\r\033[K";
        
        if (use_colors)
            std::cerr << resetColor();
    }

    void showToolCall(const std::string & tool_call_id, const std::string & function_name, const std::string & arguments = "") const
    {
        // Clear any previous line
        std::cerr << "\r\033[K";
        
        // Tool call header with icon
        if (use_colors)
            std::cerr << "\033[33m"; // Yellow
        std::cerr << "🔧 ";
        if (use_colors)
            std::cerr << "\033[1m"; // Bold
        std::cerr << "Calling: " << function_name;
        if (use_colors)
            std::cerr << "\033[0m" << "\033[33m"; // Reset bold, keep yellow
        std::cerr << " [" << tool_call_id.substr(0, 8) << "...]";
        if (use_colors)
            std::cerr << resetColor();
        std::cerr << std::endl;
        
        // Show arguments if provided (indented)
        if (!arguments.empty() && arguments != "{}")
        {
            if (use_colors)
                std::cerr << "\033[90m"; // Dark gray
            std::cerr << "  └─ Args: " << arguments;
            if (use_colors)
                std::cerr << resetColor();
            std::cerr << std::endl;
        }
    }

    void showToolResult(const std::string & function_name, const std::string & result, bool success = true) const
    {
        // Result with icon
        if (use_colors)
            std::cerr << (success ? "\033[32m" : "\033[31m"); // Green for success, Red for error
        std::cerr << (success ? "✓" : "✗") << " ";
        if (use_colors)
            std::cerr << "\033[1m"; // Bold
        std::cerr << function_name << " completed";
        if (use_colors)
            std::cerr << "\033[0m"; // Reset bold
        std::cerr << std::endl;
        
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
                std::cerr << "\033[90m"; // Dark gray
            std::cerr << "  └─ " << preview;
            if (use_colors)
                std::cerr << resetColor();
            std::cerr << std::endl;
        }
    }

    void showProgress(const std::string & message) const
    {
        if (use_colors)
            std::cerr << "\033[36m"; // Cyan
        std::cerr << "• " << message;
        if (use_colors)
            std::cerr << resetColor();
        std::cerr << std::endl;
    }

    void showSeparator() const
    {
        if (use_colors)
            std::cerr << "\033[90m"; // Dark gray
        std::cerr << "─────────────────────────────────────────────────";
        if (use_colors)
            std::cerr << resetColor();
        std::cerr << std::endl;
    }

private:
    bool use_colors;
};

}
