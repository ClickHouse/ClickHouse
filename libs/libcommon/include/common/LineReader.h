#pragma once

#include <common/Types.h>

#ifdef USE_REPLXX
#   include <replxx.hxx>
#endif

class LineReader
{
public:
    LineReader(const String & history_file_path, char extender, char delimiter = 0);  /// if delimiter != 0, then it's multiline mode
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
