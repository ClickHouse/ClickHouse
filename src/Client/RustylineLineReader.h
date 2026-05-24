#pragma once

#include <span>

#include <Client/LineReader.h>
#include <base/strong_typedef.h>

namespace DB
{

/// Line reader backed by the Rust `rustyline` crate exposed via cxx.
/// Replaces the previous `replxx`-based implementation.
class RustylineLineReader : public LineReader
{
public:
    struct Options
    {
        Suggest & suggest;
        String history_file_path;
        UInt32 history_max_entries;
        bool multiline = false;
        bool ignore_shell_suspend = false;
        bool embedded_mode = false;
        bool interactive_history_legacy_keymap = false;
        bool enable_highlight = true;
        Patterns extenders;
        Patterns delimiters;
        std::span<char> word_break_characters;
        std::istream & input_stream = std::cin;
        std::ostream & output_stream = std::cout;
        int in_fd = STDIN_FILENO;
        int out_fd = STDOUT_FILENO;
        int err_fd = STDERR_FILENO;
    };

    explicit RustylineLineReader(Options && options);
    ~RustylineLineReader() override;

    void enableBracketedPaste() override;
    void disableBracketedPaste() override;

    /// Set by the highlighter to indicate that the current buffer ends in
    /// a SQL terminator (`;` or `\G`). Drives Enter / Ctrl-J behavior.
    static void setLastIsDelimiter(bool flag);

    void setInitialText(const String & text) override;

private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;

    struct Impl;
    std::unique_ptr<Impl> impl;

    bool bracketed_paste_enabled = false;
};

}
