#pragma once

#include <common/Types.h>

#include <atomic>
#include <vector>

#ifdef USE_REPLXX
#   include <replxx.hxx>
#endif

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

    LineReader(const Suggest * suggest, const String & history_file_path, char extender, char delimiter = 0);  /// if delimiter != 0, then it's multiline mode
    ~LineReader();

    /// Reads the whole line until delimiter (in multiline mode) or until the last line without extender.
    /// If resulting line is empty, it means the user interrupted the input.
    /// Non-empty line is appended to history - without duplication.
    /// Typical delimiter is ';' (semicolon) and typical extender is '\' (backslash).
    String readLine(const String & first_prompt, const String & second_prompt);

private:
    enum InputStatus
    {
        ABORT = 0,
        RESET_LINE,
        INPUT_LINE,
    };

    String input;
    String prev_line;
    const String history_file_path;
    const char extender;
    const char delimiter;

    InputStatus readOneLine(const String & prompt);
    void addToHistory(const String & line);

#ifdef USE_REPLXX
    replxx::Replxx rx;
#endif
};
