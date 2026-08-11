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

    /// `step_number` (1-based, 0 = unknown) is the agent step this call belongs to. The indicator
    /// shows it together with the elapsed seconds so the animation reflects the turn's progress
    /// (a new step, and time ticking) rather than an identical dot loop - the model call itself is
    /// blocking and non-streaming, so this is the only live signal available during it.
    void startThinking(size_t step_number = 0)
    {
        stopThinking();

        /// The animation needs \r rewriting, which only makes sense on a terminal.
        if (!use_colors)
            return;

        thinking_active = true;
        thinking_thread = std::thread(
            [this, step_number]
            {
                const auto start = std::chrono::steady_clock::now();
                size_t dot_count = 0;
                while (thinking_active)
                {
                    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - start).count();
                    output_stream << "\r\033[K\033[36m✧ thinking";
                    if (step_number > 0)
                        output_stream << " · step " << step_number;
                    /// The trailing dots animate; keeping them last means the step/elapsed text
                    /// before them does not shift as their count changes.
                    output_stream << " · " << elapsed << "s" << std::string(dot_count, '.') << "\033[0m" << std::flush;
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
        const std::string safe = sanitizeForTerminal(text);
        if (!use_colors)
            output_stream << safe;
        else if (final)
            output_stream << renderMarkdownToANSI(safe);
        else
            output_stream << "\033[2m" << safe << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showToolCall(const std::string & tool_name, const std::string & args_summary)
    {
        if (use_colors)
            output_stream << "\033[33m";
        output_stream << "⚙ " << sanitizeForTerminal(tool_name);
        if (!args_summary.empty())
            output_stream << " " << sanitizeForTerminal(args_summary);
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showToolResult(bool success, const std::string & summary)
    {
        if (use_colors)
            output_stream << (success ? "\033[2;32m" : "\033[2;31m");
        output_stream << (success ? "  ✓ " : "  ✗ ") << oneLine(sanitizeForTerminal(summary));
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    void showError(const std::string & message)
    {
        if (use_colors)
            output_stream << "\033[31m";
        output_stream << "AI agent error: " << sanitizeForTerminal(message);
        if (use_colors)
            output_stream << "\033[0m";
        output_stream << "\n" << std::flush;
    }

    /// The model's output (and everything derived from it, like tool names or error messages
    /// quoting it) is untrusted: a prompt-injected response could carry raw ANSI/OSC escape
    /// sequences taking control of the user's terminal (clipboard writes, cursor movement,
    /// prompt spoofing). Drop all control characters except newlines and tabs; the display
    /// adds its own formatting escapes on top of the sanitized text.
    static std::string sanitizeForTerminal(const std::string & text)
    {
        std::string result;
        result.reserve(text.size());
        for (char c : text)
        {
            const auto byte = static_cast<unsigned char>(c);
            if (c == '\n' || c == '\t' || (byte >= 0x20 && byte != 0x7F))
                result += c;
        }
        return result;
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
