#pragma once

#include <Client/LineReader.h>
#include <base/strong_typedef.h>
#include <replxx.hxx>

namespace DB
{

class ReplxxLineReader : public LineReader
{
public:
    ReplxxLineReader
    (
        Suggest & suggest,
        const String & history_file_path,
        bool multiline,
        bool ignore_shell_suspend,
        Patterns extenders_,
        Patterns delimiters_,
        const char word_break_characters_[],
        replxx::Replxx::highlighter_callback_t highlighter_,
        std::istream & input_stream_ = std::cin,
        std::ostream & output_stream_ = std::cout,
        int in_fd_ = STDIN_FILENO,
        int out_fd_ = STDOUT_FILENO,
        int err_fd_ = STDERR_FILENO
    );

    ~ReplxxLineReader() override;

    void enableBracketedPaste() override;
    void disableBracketedPaste() override;

    /// If highlight is on, we will set a flag to denote whether the last token is a delimiter.
    /// This is useful to determine the behavior of <ENTER> key when multiline is enabled.
    static void setLastIsDelimiter(bool flag);
private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;
    int executeEditor(const std::string & path);
    void openEditor();

    replxx::Replxx rx;
    replxx::Replxx::highlighter_callback_t highlighter;

    const char * word_break_characters;

    // used to call flock() to synchronize multiple clients using same history file
    int history_file_fd = -1;
    bool bracketed_paste_enabled = false;

    std::string editor;
    bool overwrite_mode = false;
};

}
