#include <Client/ClientBaseHelpers.h>
#include <Client/ReplxxLineReader.h>
#include <Parsers/Lexer.h>
#include <base/errnoToString.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <unordered_set>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <functional>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <csignal>
#include <dlfcn.h>
#include <fcntl.h>
#include <fstream>
#include <filesystem>
#include <fmt/format.h>
#include <Common/quoteString.h>
#include "config.h" // USE_SKIM

#if USE_SKIM
#include <skim.h>
#endif

namespace
{

/// How many as-you-type hint rows to show at once (mirrors the Web UI completion window).
constexpr size_t HINTS_MAX_ROWS = 5;

/// Extract identifier-like words from a query so they can be prioritized in completions/hints
/// (column names, aliases, etc. typed elsewhere in the same query). Uses the SQL lexer so that
/// string literals, numbers, and comments are not mistaken for identifiers.
std::vector<std::string> extractIdentifiers(const char * text)
{
    std::vector<std::string> result;
    if (text == nullptr || *text == '\0')
        return result;

    const char * end = text + strlen(text);
    DB::Lexer lexer(text, end);
    std::unordered_set<std::string> seen;

    for (DB::Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        if (token.isError())
            break;

        std::string word;
        if (token.type == DB::TokenType::BareWord)
            word.assign(token.begin, token.end);
        else if (token.type == DB::TokenType::QuotedIdentifier && token.size() >= 2)
            word.assign(token.begin + 1, token.end - 1); /// strip the surrounding quotes/backticks

        /// The suggestion dictionary only contains words of 2+ characters.
        if (word.size() >= 2 && seen.insert(word).second)
            result.push_back(std::move(word));
    }

    return result;
}

/// Trim ending whitespace inplace
void rightTrim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), s.end());
}

std::string getEditor()
{
    const char * editor = std::getenv("EDITOR"); // NOLINT(concurrency-mt-unsafe)

    if (!editor || !*editor)
        editor = "vim";

    return editor;
}

/// See comments in ShellCommand::executeImpl()
/// (for the vfork via dlsym())
int executeCommand(char * const argv[])
{
#if !defined(USE_MUSL)
    /** Here it is written that with a normal call `vfork`, there is a chance of deadlock in multithreaded programs,
      *  because of the resolving of symbols in the shared library
      * http://www.oracle.com/technetwork/server-storage/solaris10/subprocess-136439.html
      * Therefore, separate the resolving of the symbol from the call.
      */
    static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");
#else
    /// If we use Musl with static linking, there is no dlsym and no issue with vfork.
    static void * real_vfork = reinterpret_cast<void *>(&vfork);
#endif
    if (!real_vfork)
        throw std::runtime_error("Cannot find vfork symbol");

    pid_t pid = reinterpret_cast<pid_t (*)()>(real_vfork)();

    if (-1 == pid)
        throw std::runtime_error(fmt::format("Cannot vfork {}: {}", argv[0], errnoToString()));

    /// Child
    if (0 == pid)
    {
        sigset_t mask;
        sigemptyset(&mask);
        sigprocmask(0, nullptr, &mask); // NOLINT(concurrency-mt-unsafe) // ok in newly created process
        sigprocmask(SIG_UNBLOCK, &mask, nullptr); // NOLINT(concurrency-mt-unsafe) // ok in newly created process

        execvp(argv[0], argv);
        _exit(-1);
    }

    int status = 0;
    do
    {
        int exited_pid = waitpid(pid, &status, 0);
        if (exited_pid != -1)
            break;

        if (errno == EINTR)
            continue;

        throw std::runtime_error(fmt::format("Cannot waitpid {}: {}", pid, errnoToString()));
    } while (true);

    if (WIFEXITED(status))
        return WEXITSTATUS(status);
    if (WIFSIGNALED(status))
        throw std::runtime_error(fmt::format("Child process was terminated by signal {}", WTERMSIG(status)));
    if (WIFSTOPPED(status))
        throw std::runtime_error(fmt::format("Child process was stopped by signal {}", WSTOPSIG(status)));

    throw std::runtime_error("Child process was not exited normally by unknown reason");
}

void writeRetry(int fd, const std::string & data)
{
    size_t bytes_written = 0;
    const char * begin = data.c_str();
    size_t offset = data.size();

    while (bytes_written != offset)
    {
        ssize_t res = ::write(fd, begin + bytes_written, offset - bytes_written);
        if ((-1 == res || 0 == res) && errno != EINTR)
            throw std::runtime_error(fmt::format("Cannot write to {}: {}", fd, errnoToString()));
        bytes_written += res;
    }
}
std::string readFile(const std::string & path)
{
    std::string out;
    DB::WriteBufferFromString out_buffer(out);
    DB::ReadBufferFromFile in_buffer(path);
    DB::copyData(in_buffer, out_buffer);
    return out;
}

/// Simple wrapper for temporary files.
class TemporaryFile
{
private:
    std::string path;
    int fd = -1;

public:
    explicit TemporaryFile(const char * pattern)
        : path(pattern)
    {
        size_t dot_pos = path.rfind('.');
        if (dot_pos != std::string::npos)
            fd = ::mkstemps(path.data(), static_cast<int>(path.size() - dot_pos));
        else
            fd = ::mkstemp(path.data());

        if (-1 == fd)
            throw std::runtime_error(fmt::format("Cannot create temporary file {}: {}", path, errnoToString()));
    }
    ~TemporaryFile()
    {
        try
        {
            close();
            unlink();
        }
        catch (const std::runtime_error & e)
        {
            /// musl defines `stderr` as a recursive macro `(stderr)`,
            /// which triggers `-Wdisabled-macro-expansion` when used as a function argument.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
            fmt::print(stderr, "{}", e.what());
#pragma clang diagnostic pop
        }
    }

    void close()
    {
        if (fd == -1)
            return;

        if (0 != ::close(fd))
            throw std::runtime_error(fmt::format("Cannot close temporary file {}: {}", path, errnoToString()));
        fd = -1;
    }

    void write(const std::string & data)
    {
        if (fd == -1)
            throw std::runtime_error(fmt::format("Cannot write to uninitialized file {}", path));

        writeRetry(fd, data);
    }

    void unlink()
    {
        if (0 != ::unlink(path.c_str()))
            throw std::runtime_error(fmt::format("Cannot remove temporary file {}: {}", path, errnoToString()));
    }

    std::string & getPath() { return path; }
};

/// Copied from replxx::src/util.cxx::now_ms_str() under the terms of 3-clause BSD license of Replxx.
/// Copyright (c) 2017-2018, Marcin Konarski (amok at codestation.org)
/// Copyright (c) 2010, Salvatore Sanfilippo (antirez at gmail dot com)
/// Copyright (c) 2010, Pieter Noordhuis (pcnoordhuis at gmail dot com)
std::string replxx_now_ms_str()
{
    std::chrono::milliseconds ms(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()));
    time_t t = ms.count() / 1000;
    tm broken{};
    if (!localtime_r(&t, &broken))
        return {};

    static int const BUFF_SIZE(32);
    char str[BUFF_SIZE];
    if (strftime(str, BUFF_SIZE, "%Y-%m-%d %H:%M:%S.", &broken) <= 0)
        return {};

    if (snprintf(str + sizeof("YYYY-mm-dd HH:MM:SS"), 5, "%03d", static_cast<int>(ms.count() % 1000)) <= 0)
        return {};

    return str;
}

/// Convert from readline to replxx format.
///
/// replxx requires each history line to prepended with time line:
///
///     ### YYYY-MM-DD HH:MM:SS.SSS
///     select 1
///
/// And w/o those service lines it will load all lines from history file as
/// one history line for suggestion. And if there are lots of lines in file it
/// will take lots of time (getline() + tons of reallocations).
///
/// NOTE: this code uses std::ifstream/std::ofstream like original replxx code.
void convertHistoryFile(const std::string & path, replxx::Replxx & rx)
{
    std::ifstream in(path);
    if (!in)
    {
        rx.print("Cannot open %s reading (for conversion): %s\n",
            path.c_str(), errnoToString().c_str());
        return;
    }

    std::string line;
    if (getline(in, line).bad())
    {
        rx.print("Cannot read from %s (for conversion): %s\n",
            path.c_str(), errnoToString().c_str());
        return;
    }

    /// This is the marker of the date, no need to convert.
    static char const REPLXX_TIMESTAMP_PATTERN[] = "### dddd-dd-dd dd:dd:dd.ddd";
    if (line.empty() || (line.starts_with("### ") && line.size() == strlen(REPLXX_TIMESTAMP_PATTERN)))
    {
        return;
    }

    std::vector<std::string> lines;
    in.seekg(0);
    while (getline(in, line).good())
    {
        lines.push_back(line);
    }
    in.close();

    size_t lines_size = lines.size();
    std::sort(lines.begin(), lines.end());
    lines.erase(std::unique(lines.begin(), lines.end()), lines.end());
    rx.print("The history file (%s) is in old format. %zu lines, %zu unique lines.\n",
        path.c_str(), lines_size, lines.size());

    std::ofstream out(path);
    if (!out)
    {
        rx.print("Cannot open %s for writing (for conversion): %s\n",
            path.c_str(), errnoToString().c_str());
        return;
    }

    const std::string & timestamp = replxx_now_ms_str();
    for (const auto & out_line : lines)
    {
        out << "### " << timestamp << "\n" << out_line << std::endl;
    }
    out.close();
}

}

namespace DB
{

static bool replxx_last_is_delimiter = false;
void ReplxxLineReader::setLastIsDelimiter(bool flag)
{
    replxx_last_is_delimiter = flag;
}

ReplxxLineReader::ReplxxLineReader(ReplxxLineReader::Options && options)
    : LineReader
    (
        options.history_file_path,
        options.multiline,
        std::move(options.extenders),
        std::move(options.delimiters),
        options.input_stream,
        options.output_stream,
        options.in_fd
    )
    , rx(options.input_stream, options.output_stream, options.in_fd, options.out_fd, options.err_fd)
    , highlighter(std::move(options.highlighter))
    , suggest(options.suggest)
    , word_break_characters(options.word_break_characters.data())
    , editor(getEditor())
{
    using Replxx = replxx::Replxx;

    rx.set_max_history_size(static_cast<int>(options.history_max_entries));

    if (!history_file_path.empty())
    {
        history_file_fd = open(history_file_path.c_str(), O_RDWR);
        if (history_file_fd < 0)
        {
            rx.print("Open of history file failed: %s\n", errnoToString().c_str());
        }
        else
        {
            convertHistoryFile(history_file_path, rx);

            if (flock(history_file_fd, LOCK_SH))
            {
                rx.print("Shared lock of history file failed: %s\n", errnoToString().c_str());
            }
            else
            {
                if (!rx.history_load(history_file_path))
                {
                    rx.print("Loading history failed: %s\n", errnoToString().c_str());
                }

                if (flock(history_file_fd, LOCK_UN))
                {
                    rx.print("Unlock of history file failed: %s\n", errnoToString().c_str());
                }
            }
        }
    }

    rx.install_window_change_handler();

    auto callback = [this] (const String & context, size_t context_size)
    {
        /// When this completion corresponds to the hints currently displayed, reuse the exact
        /// snapshot taken when they were shown. replxx accepts a hint by indexing this completion
        /// list with the hint selection, and the background `Suggest::load` thread can insert a
        /// word that sorts before the displayed one between display and acceptance; reusing the
        /// snapshot guarantees the accepted word is the one that was shown. Any other completion
        /// (plain Tab where no hints are shown — empty word, mid-line) is recomputed.
        if (!hint_completions.empty()
            && context == hint_completions_context
            && static_cast<int>(context_size) == hint_completions_context_size)
            return hint_completions;

        /// Prioritize identifiers already present in the whole query line (not just up to the
        /// cursor), so the same words rank first for both Tab completion and the inline hints.
        auto priority = extractIdentifiers(rx.get_state().text());
        return suggest.getCompletions(context, context_size, word_break_characters, priority);
    };

    rx.set_completion_callback(callback);

    rx.set_complete_on_empty(false);
    rx.set_word_break_characters(word_break_characters);
    rx.set_ignore_case(true);
    rx.set_indent_multiline(false);

    if (highlighter)
        rx.set_highlighter_callback(highlighter);

    /// As-you-type autocompletion: show the matching suggestions as inline "ghost" hints, with
    /// the same priority ordering as Tab completion. replxx renders a single hint inline after
    /// the cursor and a navigable list (Ctrl-Up/Ctrl-Down) for several; Tab accepts the selected
    /// one. Accepting a hint makes replxx index the *completion* list with the hint selection, so
    /// the completion callback must return the same words in the same order as the displayed hints
    /// — guaranteed here by computing the words once in the hint callback and reusing that exact
    /// snapshot in the completion callback (`hint_completions`).
    /// Hints need color, so they are only enabled together with highlighting (see ClientBase).
    if (options.enable_hints && highlighter)
    {
        auto hint_callback = [this] (const String & context, int & context_size, Replxx::Color &)
        {
            /// The callback runs only when the input text changes (replxx caches hints while it
            /// stays the same), so this is the right place to reset the navigation state and the
            /// completion snapshot.
            hint_selection = -1;
            hint_count = 0;
            hints_visible = false;
            hint_completions.clear();
            hint_completions_context.clear();
            hint_completions_context_size = 0;

            /// Mirror `set_complete_on_empty(false)` *before* matching: an empty last word matches
            /// every suggestion, and this callback runs on every zero-delay repaint, so we must not
            /// fold and stable-sort the whole dictionary only to drop the result here.
            const auto last_word_pos = context.find_last_of(word_break_characters);
            const bool last_word_empty
                = (last_word_pos == std::string::npos) ? context.empty() : (last_word_pos + 1 == context.size());
            if (last_word_empty)
                return replxx::Replxx::hints_t{};

            const std::string text = rx.get_state().text();
            auto priority = extractIdentifiers(text.c_str());
            /// Compute the matches once and cache the full list, so accepting a hint reuses exactly
            /// these words (see the completion callback). `getCompletions` returns the matches in
            /// the same priority order as the hints, so the shown hints are simply its prefix.
            hint_completions = suggest.getCompletions(context, context_size, word_break_characters, priority);
            hint_completions_context = context;
            hint_completions_context_size = context_size;

            replxx::Replxx::hints_t hints;
            const size_t shown = std::min(hint_completions.size(), HINTS_MAX_ROWS);
            hints.reserve(shown);
            for (size_t i = 0; i < shown; ++i)
                hints.push_back(hint_completions[i].text());
            hint_count = static_cast<int>(hints.size());

            /// The "popup" is active only if at least one hint actually has something to complete
            /// (a non-empty suffix). A fully-typed word matches itself with an empty suffix; that
            /// must not count, otherwise Enter would accept the no-op instead of running the query.
            for (const auto & hint : hints)
            {
                if (hint.size() > static_cast<size_t>(context_size))
                {
                    hints_visible = true;
                    break;
                }
            }
            return hints;
        };
        rx.set_hint_callback(hint_callback);
        rx.set_hint_delay(0); /// Show hints immediately, without a delay.
        rx.set_max_hint_rows(static_cast<int>(HINTS_MAX_ROWS));

        /// replxx drops its internal hint selection on every action that regenerates the line — any
        /// cursor movement, edit, etc. (see `handle_hints` on `HINT_ACTION::REGENERATE`) — but it
        /// re-invokes the hint callback only when the input *text* changes. So a plain cursor
        /// movement (e.g. Left then Right back to the end) silently clears replxx's selection while
        /// leaving our mirror stale, making Right/Enter treat a no-longer-selected hint as chosen.
        /// The modify callback runs on every dispatched action, so reset the mirror here to track
        /// replxx; the hint-navigation keys re-set it *after* invoking, so a real navigation stays.
        rx.set_modify_callback([this] (std::string &, int &) { hint_selection = -1; });
    }

    /// By default C-p/C-n bound to COMPLETE_NEXT/COMPLETE_PREV,
    /// bind C-p/C-n to history-previous/history-next like readline.
    rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_NEXT, code); });
    rx.bind_key(Replxx::KEY::control('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_PREVIOUS, code); });

    /// We don't want the default, "suspend" behavior, it confuses people.
    if (options.ignore_shell_suspend)
        rx.bind_key_internal(replxx::Replxx::KEY::control('Z'), "insert_character");

    auto commit_action = [this](char32_t code)
    {
        /// When the user has navigated to a hint, Enter accepts it (like Tab and Right) instead
        /// of running the query / inserting a newline. Only an explicitly selected hint is
        /// accepted (not a single ghost shown while typing), so Enter keeps running the query in
        /// the normal "type a command and press Enter" flow.
        if (hintPopupActive() && hint_selection >= 0)
            return rx.invoke(Replxx::ACTION::COMPLETE_LINE, code);
        /// If we allow multiline and there is already something in the input, start a newline.
        /// Also, when bytes are still queued in the TTY (paste in progress without bracketed
        /// paste support), fold the embedded newline into the same edit buffer instead of
        /// committing a partial query and switching to the continuation prompt. This way the
        /// whole paste lives in a single replxx edit buffer and arrow keys navigate across it.
        /// The paste case does not depend on the highlighter: the `NEW_LINE` action only inserts
        /// a newline into the edit buffer and does not use the lexer, so the paste stays under a
        /// single prompt even with `--highlight 0`. The explicit `--multiline` continuation still
        /// requires the highlighter, preserving the previous behavior.
        if (!replxx_last_is_delimiter && ((highlighter && multiline) || hasInputData()))
            return rx.invoke(Replxx::ACTION::NEW_LINE, code);
        replxx_last_is_delimiter = false;
        return rx.invoke(Replxx::ACTION::COMMIT_LINE, code);
    };
    /// bind C-j to ENTER action.
    rx.bind_key(Replxx::KEY::control('J'), commit_action);
    rx.bind_key(Replxx::KEY::ENTER, commit_action);

    /// By default COMPLETE_NEXT/COMPLETE_PREV was bound to C-p/C-n, re-bind
    /// to M-P/M-N (that was used for HISTORY_COMMON_PREFIX_SEARCH before, but
    /// it also bound to M-p/M-n).
    rx.bind_key(Replxx::KEY::meta('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_NEXT, code); });
    rx.bind_key(Replxx::KEY::meta('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_PREVIOUS, code); });
    /// By default M-BACKSPACE is KILL_TO_WHITESPACE_ON_LEFT, while in readline it is backward-kill-word
    rx.bind_key(Replxx::KEY::meta(Replxx::KEY::BACKSPACE), [this](char32_t code) { return rx.invoke(Replxx::ACTION::KILL_TO_BEGINING_OF_WORD, code); });
    /// By default C-w is KILL_TO_BEGINING_OF_WORD, while in readline it is unix-word-rubout
    rx.bind_key(Replxx::KEY::control('W'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::KILL_TO_WHITESPACE_ON_LEFT, code); });

    /// When the as-you-type hints are shown, let the arrow keys drive them like the Web UI
    /// completion popup: Down steps into / advances the hint list, Up moves back through it, and
    /// Tab/Right/Enter accept the chosen hint. Up only navigates the hints once one is selected;
    /// before that Up keeps recalling command history, so the hints never shadow it. Right and
    /// Enter act only on a chosen hint and never pop the old-style completion list (only Tab does
    /// that). Outside the popup these keys behave normally. Ctrl-Up/Ctrl-Down (replxx's defaults)
    /// are rebound to the same logic so the selection stays in sync. Note: Esc cannot be used to
    /// dismiss the hints, because the bundled replxx reads a lone Esc by blocking for the next
    /// byte (no escape-key timeout); the Up-to-history behavior above covers getting back to
    /// history instead.
    if (options.enable_hints && highlighter)
    {
        /// Down advances the selection (and steps into the list); these mirror replxx's internal
        /// wrap (past the last hint -> nothing selected -> first hint) so we know which hint, if
        /// any, is currently chosen.
        /// `rx.invoke` runs the modify callback, which resets `hint_selection`, so compute the new
        /// selection from the current one *before* invoking and re-apply it *after* — this keeps
        /// our mirror equal to replxx's internal selection (which the same action advances).
        auto hint_next = [this](char32_t code)
        {
            if (hintPopupActive())
            {
                const int next = (hint_selection + 1 >= hint_count) ? -1 : hint_selection + 1;
                auto result = rx.invoke(Replxx::ACTION::HINT_NEXT, code);
                hint_selection = next;
                return result;
            }
            return rx.invoke(Replxx::ACTION::LINE_NEXT, code);
        };
        /// Up navigates the hints only once a hint is selected; before that it keeps recalling
        /// command history, so the hints do not shadow it.
        auto hint_previous = [this](char32_t code)
        {
            if (hintPopupActive() && hint_selection >= 0)
            {
                const int next = hint_selection - 1; /// from the first hint this deselects; next Up recalls history
                auto result = rx.invoke(Replxx::ACTION::HINT_PREVIOUS, code);
                hint_selection = next;
                return result;
            }
            return rx.invoke(Replxx::ACTION::LINE_PREVIOUS, code);
        };
        rx.bind_key(Replxx::KEY::DOWN, hint_next);
        rx.bind_key(Replxx::KEY::UP, hint_previous);
        /// Ctrl-Up/Ctrl-Down explicitly drive the hints (Ctrl-Up also enters the list from the end).
        rx.bind_key(Replxx::KEY::control(Replxx::KEY::DOWN), hint_next);
        rx.bind_key(Replxx::KEY::control(Replxx::KEY::UP), [this](char32_t code)
        {
            if (hintPopupActive())
            {
                const int next = (hint_selection - 1 < -1) ? hint_count - 1 : hint_selection - 1;
                auto result = rx.invoke(Replxx::ACTION::HINT_PREVIOUS, code);
                hint_selection = next;
                return result;
            }
            return rx.invoke(Replxx::ACTION::LINE_PREVIOUS, code);
        });

        /// Right accepts the chosen hint (the single one shown, or the one selected by navigating);
        /// it never triggers the old-style completion list. Otherwise it just moves the cursor.
        rx.bind_key(Replxx::KEY::RIGHT, [this](char32_t code)
        {
            if (hintChosen())
                return rx.invoke(Replxx::ACTION::COMPLETE_LINE, code);
            return rx.invoke(Replxx::ACTION::MOVE_CURSOR_RIGHT, code);
        });
    }

    /// We don't want to allow opening EDITOR in the embedded mode.
    if (!options.embedded_mode)
    {
        rx.bind_key(Replxx::KEY::meta('E'), [this](char32_t) { openEditor(/*format_query=*/ false); return Replxx::ACTION_RESULT::CONTINUE; });
        rx.bind_key(Replxx::KEY::meta('F'), [this](char32_t) { openEditor(/*format_query=*/ true); return Replxx::ACTION_RESULT::CONTINUE; });
    }

    /// readline insert-comment
    auto insert_comment_action = [this](char32_t code)
    {
        replxx::Replxx::State state(rx.get_state());
        const char * line = state.text();
        const char * line_end = line + strlen(line);

        std::string commented_line;
        if (std::find(line, line_end, '\n') != line_end)
        {
            /// If query has multiple lines, multiline comment is used over
            /// commenting each line separately for easier uncomment (though
            /// with invoking editor it is simpler to uncomment multiple lines)
            ///
            /// Note, that using multiline comment is OK even with nested
            /// comments, since nested comments are supported.
            commented_line = fmt::format("/* {} */", state.text());
        }
        else
        {
            // In a simplest case use simple comment.
            commented_line = fmt::format("-- {}", state.text());
        }
        rx.set_state(replxx::Replxx::State(commented_line.c_str(), static_cast<int>(commented_line.size())));

        return rx.invoke(Replxx::ACTION::COMMIT_LINE, code);
    };
    rx.bind_key(Replxx::KEY::meta('#'), insert_comment_action);

    char key_fuzzy = 'R';
    char key_regular = 'T';
    if (options.interactive_history_legacy_keymap)
        std::swap(key_fuzzy, key_regular);

#if USE_SKIM
    if (!options.embedded_mode)
    {
        auto interactive_history_search = [this, key_regular](char32_t code)
        {
            std::vector<std::string> words;
            {
                auto hs(rx.history_scan());
                while (hs.next())
                    words.push_back(hs.get().text());
            }

            std::string current_query(rx.get_state().text());
            std::string new_query;
            try
            {
                new_query = std::string(skim(current_query, words));
            }
            catch (const std::exception & e)
            {
                rx.print("skim failed: %s (consider using Ctrl-%c for a regular non-fuzzy reverse search)\n", e.what(), key_regular);
            }

            /// REPAINT before to avoid prompt overlap by the query
            rx.invoke(Replxx::ACTION::REPAINT, code);

            if (!new_query.empty())
                rx.set_state(replxx::Replxx::State(new_query.c_str(), static_cast<int>(new_query.size())));

            if (bracketed_paste_enabled)
                enableBracketedPaste();

            rx.invoke(Replxx::ACTION::CLEAR_SELF, code);
            return rx.invoke(Replxx::ACTION::REPAINT, code);
        };

        rx.bind_key(Replxx::KEY::control(key_fuzzy), interactive_history_search);
    }
#endif

    /// Rebind regular incremental search.
    ///
    /// NOTE: C-T by default this is a binding to swap adjustent chars
    /// (TRANSPOSE_CHARACTERS), but for SQL it sounds pretty useless.
    rx.bind_key(Replxx::KEY::control(key_regular), [this](char32_t)
    {
        /// Reverse search is detected by C-R.
        uint32_t reverse_search = Replxx::KEY::control('R');
        return rx.invoke(Replxx::ACTION::HISTORY_INCREMENTAL_SEARCH, reverse_search);
    });

    /// Change cursor style for overwrite mode to blinking (see console_codes(5))
    rx.bind_key(Replxx::KEY::INSERT, [this](char32_t)
    {
        overwrite_mode = !overwrite_mode;
        if (overwrite_mode)
            rx.print("%s", "\033[5 q");
        else
            rx.print("%s", "\033[0 q");
        return rx.invoke(Replxx::ACTION::TOGGLE_OVERWRITE_MODE, 0);
    });
}

bool ReplxxLineReader::isCursorAtEndOfInput()
{
    const replxx::Replxx::State state(rx.get_state());
    const char * text = state.text();
    /// replxx cursor positions are counted in code points; count them in the UTF-8 text.
    size_t code_points = 0;
    for (const char * p = text; *p != '\0'; ++p)
        if ((static_cast<unsigned char>(*p) & 0xC0) != 0x80)
            ++code_points;
    return state.cursor_position() >= static_cast<int>(code_points);
}

bool ReplxxLineReader::hintPopupActive()
{
    /// Treat Up/Down as hint navigation only where the hints are actually shown — at the end of
    /// the input. Hints are shown only at the end of the buffer (including the last line of a
    /// multi-line query), so elsewhere this returns false and Up/Down keep moving between lines
    /// and through history.
    return hints_visible && isCursorAtEndOfInput();
}

bool ReplxxLineReader::hintChosen()
{
    /// A hint is "chosen" when there is a single hint shown (the ghost) or the user has selected
    /// one by navigating. In both cases accepting it inserts text rather than popping the
    /// old-style completion list.
    return hintPopupActive() && (hint_selection >= 0 || hint_count == 1);
}

ReplxxLineReader::~ReplxxLineReader()
{
    if (history_file_fd >= 0 && close(history_file_fd))
        rx.print("Close of history file failed: %s\n", errnoToString().c_str());

    /// Reset cursor blinking
    if (overwrite_mode)
        rx.print("%s", "\033[0 q");
}

LineReader::InputStatus ReplxxLineReader::readOneLine(const String & prompt)
{
    input.clear();

    const char* cinput = rx.input(prompt);
    if (cinput == nullptr)
        return (errno != EAGAIN) ? ABORT : RESET_LINE;
    input = cinput;

    rightTrim(input);
    return INPUT_LINE;
}

void ReplxxLineReader::addToHistory(const String & line)
{
    // locking history file to prevent from inconsistent concurrent changes
    //
    // replxx::Replxx::history_save() already has lockf(),
    // but replxx::Replxx::history_load() does not
    // and that is why flock() is added here.
    bool locked = false;
    if (history_file_fd >= 0 && flock(history_file_fd, LOCK_EX))
        rx.print("Lock of history file failed: %s\n", errnoToString().c_str());
    else
        locked = true;

    rx.history_add(line);

    /// Remember identifiers from the committed query so they are prioritized in later
    /// completions/hints this session (the "previously used" tier).
    suggest.addUsedWords(extractIdentifiers(line.c_str()));

    // flush changes to the disk
    if (history_file_fd >= 0 && !rx.history_save(history_file_path))
        rx.print("Saving history failed: %s\n", errnoToString().c_str());

    if (history_file_fd >= 0 && locked && 0 != flock(history_file_fd, LOCK_UN))
        rx.print("Unlock of history file failed: %s\n", errnoToString().c_str());
}

void ReplxxLineReader::openEditor(bool format_query)
{
    /// We need to clear till the end of screen *before*, to avoid extra new-line in case of multi-line queries
    rx.invoke(replxx::Replxx::ACTION::CLEAR_SELF, 0);

    try
    {
        String query = rx.get_state().text();

        if (format_query)
            query = formatQuery(std::move(query));

        TemporaryFile editor_file("clickhouse_client_editor_XXXXXX.sql");
        editor_file.write(query);
        editor_file.close();

        char * const argv[] = {editor.data(), editor_file.getPath().data(), nullptr};

        int editor_exit_code = executeCommand(argv);
        if (editor_exit_code == EXIT_SUCCESS)
        {
            const std::string & new_query = readFile(editor_file.getPath());
            rx.set_state(replxx::Replxx::State(new_query.c_str(), static_cast<int>(new_query.size())));
        }
        else
        {
            rx.print(fmt::format("Editor {} terminated unsuccessfully: {}\n", backQuoteIfNeed(editor), editor_exit_code).data());
        }
    }
    catch (const std::exception & e)
    {
        rx.print("\n");
        rx.print(e.what());
        rx.print("\n");
    }

    rx.invoke(replxx::Replxx::ACTION::CLEAR_SELF, 0);
    rx.invoke(replxx::Replxx::ACTION::REPAINT, 0);

    if (bracketed_paste_enabled)
        enableBracketedPaste();
}

void ReplxxLineReader::enableBracketedPaste()
{
    bracketed_paste_enabled = true;
    rx.enable_bracketed_paste();
}

void ReplxxLineReader::disableBracketedPaste()
{
    bracketed_paste_enabled = false;
    rx.disable_bracketed_paste();
}

void ReplxxLineReader::setInitialText(const String & text)
{
    // Preload the buffer with the initial text
    if (!text.empty())
    {
        rx.set_preload_buffer(text);
    }
}

}
