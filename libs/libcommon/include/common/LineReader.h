#pragma once

#include <common/Types.h>

#include <atomic>
#include <vector>

class LineReader
{
public:
    class Suggest
    {
    protected:
        using Words = std::vector<std::string>;
        using WordsRange = std::pair<Words::const_iterator, Words::const_iterator>;

        Words words;
        std::atomic<bool> ready{false};

    public:
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
