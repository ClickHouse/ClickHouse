#pragma once

#include <span>

#include <Client/LineReader.h>
#include <base/strong_typedef.h>
#include <replxx.hxx>

namespace DB
{

class ReplxxLineReader : public LineReader
{
public:
    /// The highlighter callback of replxx, which also reports whether the last token of the
    /// query is a delimiter: it sets `last_is_delimiter` to true or false, or leaves it intact
    /// when this cannot be determined (e.g. the query ends with whitespace).
    using Highlighter = std::function<void(const String & query, std::vector<replxx::Replxx::Color> & colors, int cursor_position, bool & last_is_delimiter)>;

    struct Options
    {
        Suggest & suggest;
        String history_file_path;
        UInt32 history_max_entries;
        bool multiline = false;
        bool ignore_shell_suspend = false;
        bool embedded_mode = false;
        bool interactive_history_legacy_keymap = false;
        Patterns extenders;
        Patterns delimiters;
        std::span<char> word_break_characters;
        Highlighter highlighter;
        std::istream & input_stream = std::cin;
        std::ostream & output_stream = std::cout;
        int in_fd = STDIN_FILENO;
        int out_fd = STDOUT_FILENO;
        int err_fd = STDERR_FILENO;
    };

    explicit ReplxxLineReader(Options && options);
    ~ReplxxLineReader() override;

    void enableBracketedPaste() override;
    void disableBracketedPaste() override;

    /// Set text to be prepopulated in the next readLine call
    void setInitialText(const String & text) override;
private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;
    int executeEditor(const std::string & path);
    void openEditor(bool format_query);

    replxx::Replxx rx;
    Highlighter highlighter;

    /// If highlight is on, the highlighter reports whether the last token is a delimiter.
    /// This is used to determine the behavior of <ENTER> key when multiline is enabled.
    /// It is a member rather than a global, because several readers can work concurrently
    /// in one process (client sessions embedded into the server).
    bool last_is_delimiter = false;

    const char * word_break_characters;

    // used to call flock() to synchronize multiple clients using same history file
    int history_file_fd = -1;
    bool bracketed_paste_enabled = false;

    std::string editor;
    bool overwrite_mode = false;
};

}
