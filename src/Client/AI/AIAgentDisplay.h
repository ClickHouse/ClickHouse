#pragma once

#include <Client/AI/MarkdownFormatter.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ostream>
#include <string>
#include <thread>

namespace DB
{

/// Terminal rendering of the AI agent activity: the model's commentary ("thoughts"),
/// tool calls and their results, and a thinking animation while a model response
/// is being generated. Queries the agent runs are displayed elsewhere, through the
/// normal query output path of the client.
class AIAgentDisplay
{
public:
    AIAgentDisplay(std::ostream & output_stream_, bool use_colors_)
        : output_stream(output_stream_), use_colors(use_colors_)
    {
    }

    ~AIAgentDisplay()
    {
        stopThinking();
    }

    void startThinking()
    {
        stopThinking();

        /// The animation needs \r rewriting, which only makes sense on a terminal.
        if (!use_colors)
            return;

        thinking_active = true;
        thinking_thread = std::thread(
            [this]
            {
                size_t dot_count = 0;
                while (thinking_active)
                {
                    output_stream << "\r\033[K\033[36m✧ thinking" << std::string(dot_count, '.') << "\033[0m" << std::flush;
                    std::this_thread::sleep_for(std::chrono::milliseconds(400));
                    dot_count = (dot_count + 1) % 4;
                }
                output_stream << "\r\033[K" << std::flush;
            });
    }

    void stopThinking()
    {
        if (thinking_active.exchange(false))
        {
            if (thinking_thread.joinable())
                thinking_thread.join();
        }
    }

    /// The model's commentary. Intermediate thoughts (between tool calls) are dimmed;
    /// the final answer is rendered as Markdown (the format the model writes in) when the
    /// output is a terminal, and printed raw otherwise.
    void showAssistantText(const std::string & text, bool final)
    {
        if (text.empty())
            return;
        if (!use_colors)
            output_stream << text;
        else if (final)
            output_stream << renderMarkdownToANSI(text);
        else
            output_stream << "\033[2m" << text << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showToolCall(const std::string & tool_name, const std::string & args_summary)
    {
        if (use_colors)
            output_stream << "\033[33m";
        output_stream << "⚙ " << tool_name;
        if (!args_summary.empty())
            output_stream << " " << args_summary;
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showToolResult(bool success, const std::string & summary)
    {
        if (use_colors)
            output_stream << (success ? "\033[2;32m" : "\033[2;31m");
        output_stream << (success ? "  ✓ " : "  ✗ ") << oneLine(summary);
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showError(const std::string & message)
    {
        if (use_colors)
            output_stream << "\033[31m";
        output_stream << "AI agent error: " << message;
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showNotice(const std::string & message)
    {
        if (use_colors)
            output_stream << "\033[36m";
        output_stream << message;
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

private:
    /// Single line, truncated: tool results can be screens of text, and their full content
    /// is for the model; the user only needs a hint of what happened.
    static std::string oneLine(std::string text)
    {
        static constexpr size_t max_length = 120;
        std::replace(text.begin(), text.end(), '\n', ' ');
        if (text.size() > max_length)
        {
            text.resize(max_length);
            text += "…";
        }
        return text;
    }

    std::ostream & output_stream;
    bool use_colors;
    std::atomic<bool> thinking_active{false};
    std::thread thinking_thread;
};

}
