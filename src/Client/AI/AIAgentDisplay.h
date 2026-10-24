#pragma once

#include <Client/TerminalMarkdownRenderer.h>
#include <Common/TerminalSize.h>

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
        {
            /// The final answer is a Markdown document: it is rendered with the same renderer
            /// the `help` command uses for the embedded documentation, word-wrapped to the
            /// terminal width.
            TerminalMarkdownRenderer renderer;
            try
            {
                const uint16_t detected_width = getTerminalWidth();
                if (detected_width >= 20)
                    renderer.width = detected_width;
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Ok: if the terminal width cannot be determined, the default is used.
            }
            String rendered = renderer.render(safe);
            /// `render` ends with a newline of its own; the common one is appended below.
            if (rendered.ends_with('\n'))
                rendered.pop_back();
            output_stream << rendered;
        }
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
    /// adds its own formatting escapes on top of the sanitized text. Besides the C0 controls
    /// and DEL, the C1 controls U+0080..U+009F (UTF-8 `C2 80`..`C2 9F`) are dropped as well:
    /// on terminals that honor them, `U+009B`/`U+009D` open CSI/OSC sequences without any ESC
    /// byte. Stray non-UTF-8 bytes in that range are dropped too (an 8-bit terminal could
    /// interpret them as C1), while continuation bytes of valid multi-byte sequences are kept.
    static std::string sanitizeForTerminal(const std::string & text)
    {
        std::string result;
        result.reserve(text.size());
        const size_t size = text.size();
        size_t i = 0;
        while (i < size)
        {
            const auto byte = static_cast<unsigned char>(text[i]);
            if (byte < 0x80)
            {
                if (text[i] == '\n' || text[i] == '\t' || (byte >= 0x20 && byte != 0x7F))
                    result += text[i];
                ++i;
            }
            else if (byte >= 0xC0)
            {
                /// A multi-byte sequence: copied whole, so its continuation bytes (which fall
                /// into 0x80..0xBF by construction) are never confused with standalone C1 bytes.
                size_t length = byte >= 0xF0 ? 4 : (byte >= 0xE0 ? 3 : 2);
                size_t actual = 1;
                while (actual < length && i + actual < size
                       && (static_cast<unsigned char>(text[i + actual]) & 0xC0) == 0x80)
                    ++actual;
                const bool is_c1_control = byte == 0xC2 && actual == 2
                    && static_cast<unsigned char>(text[i + 1]) <= 0x9F;
                if (!is_c1_control)
                    result.append(text, i, actual);
                i += actual;
            }
            else
            {
                /// A stray continuation byte outside of a multi-byte sequence: invalid UTF-8,
                /// and 0x80..0x9F could still be honored as a C1 control - drop it.
                ++i;
            }
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
