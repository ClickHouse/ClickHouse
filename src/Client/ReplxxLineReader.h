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
        /// Use the asynchronously loaded suggestion dictionary for as-you-type hints. Client
        /// slash commands are static and remain available when this is disabled.
        bool enable_suggestion_hints = true;
        /// Offer the `/`-commands of the client (`/help`, `/clear`, ...) at the beginning of the
        /// input, as hints and as Tab completions. Off by default: `keeper-client` and
        /// `clickhouse-disks` reuse this reader and have commands of their own, so a leading `/`
        /// means something else there (the beginning of a path).
        bool enable_slash_commands = false;
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

    /// Run a history-navigation action with the hint suppression armed (see
    /// `suppress_hints_once`): the entry it recalls must not pop hints by itself.
    replxx::Replxx::ACTION_RESULT historyNavigate(replxx::Replxx::ACTION action, char32_t code);

    /// Run a history-search action with the hint suppression armed (see
    /// `suppress_hints_once`): a selected entry must not pop hints by itself.
    replxx::Replxx::ACTION_RESULT historySearch(replxx::Replxx::ACTION action, char32_t code);

    /// After a line was displayed programmatically, pin its text so that any hint regeneration
    /// for it shows nothing (see `suppress_hints_for_text`).
    void suppressHintsForDisplayedLine();

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

    /// Whether the `/`-commands of the client are offered as hints and completions (see Options).
    const bool enable_slash_commands;
    /// Whether ordinary as-you-type hints may query the asynchronously loaded suggestion dictionary.
    const bool enable_suggestion_hints;

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

    /// Suppression of the as-you-type hints for a line that is displayed programmatically -
    /// recalled from history, found by a history search, pasted, brought back from the editor.
    /// Such a display must not pop hints by itself: with hints visible, the next Up/Down press
    /// would navigate the hints instead of the history. An edit shows the hints again.
    /// `suppress_hints_once` is armed before the action that displays the line (the action
    /// repaints, and regenerates the hints, inside itself) and consumed by the next run of the
    /// hint callback. `suppress_hints_for_text` then pins the displayed text after the action,
    /// because the same display can regenerate the hints again later - replxx replays a
    /// throttled refresh after the key handler returns (its "rapid refresh" of e.g. a held-down
    /// Up key) - so any later callback run for exactly this text shows no hints either; the
    /// first run for an edited text clears the pin.
    bool suppress_hints_once = false;
    std::string suppress_hints_for_text;

    /// Snapshot of the completion words computed when the hints were last displayed, plus the
    /// context (prefix and its length) they were computed for. The completion callback reuses it
    /// so that accepting a hint inserts exactly the word that was shown: replxx accepts a hint by
    /// indexing the *completion* list with the hint selection, and the background `Suggest::load`
    /// thread can otherwise insert a word that sorts before the displayed one between display and
    /// acceptance. Input-thread only, like the rest of the hint state.
    replxx::Replxx::completions_t hint_completions;
    std::string hint_completions_context;
    int hint_completions_context_size = 0;
};

}
