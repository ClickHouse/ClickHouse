#pragma once

#include <common/types.h>

#include <atomic>
#include <vector>

class LineReader
{
public:
    struct Suggest
    {
        using Words = std::vector<std::string>;
        using WordsRange = std::pair<Words::const_iterator, Words::const_iterator>;

        Words words;
        Words words_no_case;
        std::atomic<bool> ready{false};

        /// Get iterators for the matched range of words if any.
        WordsRange getCompletions(const String & prefix, size_t prefix_length) const;
    };

    LineReader(const String & history_file_path, char extender, char delimiter = 0);  /// if delimiter != 0, then it's multiline mode
    virtual ~LineReader() {}

    /// Reads the whole line until delimiter (in multiline mode) or until the last line without extender.
    /// If resulting line is empty, it means the user interrupted the input.
    /// Non-empty line is appended to history - without duplication.
    /// Typical delimiter is ';' (semicolon) and typical extender is '\' (backslash).
    String readLine(const String & first_prompt, const String & second_prompt);

    /// When bracketed paste mode is set, pasted text is bracketed with control sequences so
    /// that the program can differentiate pasted text from typed-in text. This helps
    /// clickhouse-client so that without -m flag, one can still paste multiline queries, and
    /// possibly get better pasting performance. See https://cirw.in/blog/bracketed-paste for
    /// more details.
    virtual void enableBracketedPaste() {}

protected:
    enum InputStatus
    {
        ABORT = 0,
        RESET_LINE,
        INPUT_LINE,
    };

    const String history_file_path;
    static constexpr char word_break_characters[] = " \t\n\r\"\\'`@$><=;|&{(.";

    String input;

private:
    const char extender;
    const char delimiter;

    String prev_line;

    virtual InputStatus readOneLine(const String & prompt);
    virtual void addToHistory(const String &) {}
};
