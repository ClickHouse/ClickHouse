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
    struct Options
    {
        Suggest & suggest;
        String history_file_path;
        UInt32 history_max_entries;
        bool multiline = false;
        bool ignore_shell_suspend = false;
        bool embedded_mode = false;
        bool interactive_history_legacy_keymap = false;
        /// Show as-you-type autocompletion hints (ghost text). Requires color (highlighting).
        bool enable_hints = true;
        Patterns extenders;
        Patterns delimiters;
        std::span<char> word_break_characters;
        replxx::Replxx::highlighter_callback_with_pos_t highlighter;
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

    /// If highlight is on, we will set a flag to denote whether the last token is a delimiter.
    /// This is useful to determine the behavior of <ENTER> key when multiline is enabled.
    static void setLastIsDelimiter(bool flag);

    /// Set text to be prepopulated in the next readLine call
    void setInitialText(const String & text) override;
private:
    InputStatus readOneLine(const String & prompt) override;
    void addToHistory(const String & line) override;
    int executeEditor(const std::string & path);
    void openEditor(bool format_query);

    /// Bind the vim-style normal/operator/motion keymap. Defined in VimMode.cpp.
    void setupVimKeybindings();
    void fixTrailingNewline(int *pos, std::string *text);
    void resetVim(int *pos = nullptr, std::string *text = nullptr);
    template <typename T>
    void bindKey(char32_t key, T && func, int mode);
    void recomputeCurswant(int pos, std::string &text);
    int vimReps() const;
    void vimWordMotion(int &pos, std::string &text, char motion);
    void find(std::string &text, int &pos, char direction, char c);

    /// Whether the text cursor is at the very end of the input (where as-you-type hints render).
    bool isCursorAtEndOfInput();
    /// Whether the as-you-type hint "popup" is currently navigable here: hints are shown and the
    /// cursor is at the end of the input (including the last line of a multi-line query). When it
    /// is not, Up/Down keep moving between lines and through history.
    bool hintPopupActive();
    /// Whether a specific hint is currently chosen, so Right/Enter accept it (and Tab too). This
    /// is the single shown hint, or one the user has selected by navigating. When no hint is
    /// chosen, only Tab triggers the (old-style) completion list; Right/Enter do nothing special.
    bool hintChosen();

    replxx::Replxx rx;
    replxx::Replxx::highlighter_callback_with_pos_t highlighter;

    /// Suggestion dictionary; used both for Tab completion and as-you-type hints, and updated
    /// with identifiers from committed queries to prioritize recently-used words.
    Suggest & suggest;

    const char * word_break_characters;

    // used to call flock() to synchronize multiple clients using same history file
    int history_file_fd = -1;
    bool bracketed_paste_enabled = false;

    std::string editor;
    bool overwrite_mode = false;

    /// As-you-type hint state (input-thread only). `hints_visible` is whether a hint with
    /// something to complete is currently shown. `hint_count` is how many hints are shown and
    /// `hint_selection` which one is selected (0-based, -1 = none) — kept in sync with replxx's
    /// internal selection so Right/Enter can tell whether a specific hint is chosen. All three
    /// are refreshed when the input changes (the hint callback regenerates); `hint_selection` is
    /// additionally reset by the modify callback on every action, to track replxx's own reset.
    bool hints_visible = false;
    int hint_count = 0;
    int hint_selection = -1;

    /// Snapshot of the completion words computed when the hints were last displayed, plus the
    /// context (prefix and its length) they were computed for. The completion callback reuses it
    /// so that accepting a hint inserts exactly the word that was shown: replxx accepts a hint by
    /// indexing the *completion* list with the hint selection, and the background `Suggest::load`
    /// thread can otherwise insert a word that sorts before the displayed one between display and
    /// acceptance. Input-thread only, like the rest of the hint state.
    replxx::Replxx::completions_t hint_completions;
    std::string hint_completions_context;
    int hint_completions_context_size = 0;

    /// Vim keymap state. `vimbuffer` accumulates the count typed before an operator (e.g. the 3 in
    /// `3w`), `vimbufferinner` the count typed after an operator (the 2 in `d2w`); the effective
    /// repeat is their product. The editing mode itself is stored in replxx (see set_editing_mode)
    /// and encodes the pending operator and motion prefix as a bitfield of the flags below.
    enum {
        MODE_INSERT,
        MODE_NORMAL,
        MODE_FIND,
        MODE_REPLACE,
        MODE_G,
        MODE_END,
    };

    enum {
        OPERATOR_C = 1,
        OPERATOR_D,
    };

    enum {
        FLAG_INSIDE = 0x1,
        FLAG_AROUND = 0x2,
    };

    uint64_t vimbuffer = 0;
    uint64_t vimbufferinner = 0;
    int32_t flag = 0;
    char find_direction = 0;
    int32_t op = 0;
    int curswant = 0;
    int inclusivity_flip = 0;

    char last_find_char = 0;
    char last_find_direction = 0;
};

}
