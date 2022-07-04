#pragma once

#include <mutex>
#include <atomic>
#include <vector>
#include <optional>
#include <replxx.hxx>

#include <base/types.h>

class LineReader
{
public:
    struct Suggest
    {
        using Words = std::vector<std::string>;

        /// Get vector for the matched range of words if any.
        replxx::Replxx::completions_t getCompletions(const String & prefix, size_t prefix_length);
        void addWords(Words && new_words);

    private:
        Words words;
        Words words_no_case;

        std::mutex mutex;
    };

    using Patterns = std::vector<const char *>;

    LineReader(const String & history_file_path, bool multiline, Patterns extenders, Patterns delimiters);
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
    static constexpr char word_break_characters[] = " \t\v\f\a\b\r\n`~!@#$%^&*()-=+[{]}\\|;:'\",<.>/?";

    String input;

    bool multiline;

    Patterns extenders;
    Patterns delimiters;

    String prev_line;

    virtual InputStatus readOneLine(const String & prompt);
    virtual void addToHistory(const String &) {}
};
