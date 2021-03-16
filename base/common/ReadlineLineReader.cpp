#include <common/ReadlineLineReader.h>
#include <ext/scope_guard.h>

#include <errno.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

#include <iostream>

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

}

static const LineReader::Suggest * suggest;

/// Points to current word to suggest.
static LineReader::Suggest::Words::const_iterator pos;
/// Points after the last possible match.
static LineReader::Suggest::Words::const_iterator end;

/// Set iterators to the matched range of words if any.
static void findRange(const char * prefix, size_t prefix_length)
{
    std::string prefix_str(prefix);
    if (auto completions = suggest->getCompletions(prefix_str, prefix_length))
        std::tie(pos, end) = *completions;
}

/// Iterates through matched range.
static char * nextMatch()
{
    if (pos >= end)
        return nullptr;

    /// readline will free memory by itself.
    char * word = strdup(pos->c_str());
    ++pos;
    return word;
}

static char * generate(const char * text, int state)
{
    if (!suggest->ready)
        return nullptr;
    if (state == 0)
        findRange(text, strlen(text));

    /// Do not append whitespace after word. For unknown reason, rl_completion_append_character = '\0' does not work.
    rl_completion_suppress_append = 1;

    return nextMatch();
};

ReadlineLineReader::ReadlineLineReader(
    const Suggest & suggest_, const String & history_file_path_, bool multiline_, Patterns extenders_, Patterns delimiters_)
    : LineReader(history_file_path_, multiline_, std::move(extenders_), std::move(delimiters_))
{
    suggest = &suggest_;

    if (!history_file_path.empty())
    {
        int res = read_history(history_file_path.c_str());
        if (res)
            std::cerr << "Cannot read history from file " + history_file_path + ": "+ strerror(errno) << std::endl;
    }

    /// Added '.' to the default list. Because it is used to separate database and table.
    rl_basic_word_break_characters = word_break_characters;

    /// Not append whitespace after single suggestion. Because whitespace after function name is meaningless.
    rl_completion_append_character = '\0';

    rl_completion_entry_function = generate;

    /// Install Ctrl+C signal handler that will be used in interactive mode.

    if (rl_initialize())
        throw std::runtime_error("Cannot initialize readline");

    auto clear_prompt_or_exit = [](int)
    {
        /// This is signal safe.
        ssize_t res = write(STDOUT_FILENO, "\n", 1);

        /// Allow to quit client while query is in progress by pressing Ctrl+C twice.
        /// (First press to Ctrl+C will try to cancel query by InterruptListener).
        if (res == 1 && rl_line_buffer[0] && !RL_ISSTATE(RL_STATE_DONE))
        {
            rl_replace_line("", 0);
            if (rl_forced_update_display())
                _exit(0);
        }
        else
        {
            /// A little dirty, but we struggle to find better way to correctly
            /// force readline to exit after returning from the signal handler.
            _exit(0);
        }
    };

    if (signal(SIGINT, clear_prompt_or_exit) == SIG_ERR)
        throw std::runtime_error(std::string("Cannot set signal handler for readline: ") + strerror(errno));

    rl_variable_bind("completion-ignore-case", "on");
    // TODO: it doesn't work
    // history_write_timestamps = 1;
}

ReadlineLineReader::~ReadlineLineReader()
{
}

LineReader::InputStatus ReadlineLineReader::readOneLine(const String & prompt)
{
    input.clear();

    const char* cinput = readline(prompt.c_str());
    if (cinput == nullptr)
        return (errno != EAGAIN) ? ABORT : RESET_LINE;
    input = cinput;

    trim(input);
    return INPUT_LINE;
}

void ReadlineLineReader::addToHistory(const String & line)
{
    add_history(line.c_str());

    // Flush changes to the disk
    // NOTE readline builds a buffer of all the lines to write, and write them in one syscall.
    // Thus there is no need to lock the history file here.
    write_history(history_file_path.c_str());
}

#if RL_VERSION_MAJOR >= 7

#define BRACK_PASTE_PREF "\033[200~"
#define BRACK_PASTE_SUFF "\033[201~"

#define BRACK_PASTE_LAST '~'
#define BRACK_PASTE_SLEN 6

/// This handler bypasses some unused macro/event checkings and remove trailing newlines before insertion.
static int clickhouse_rl_bracketed_paste_begin(int /* count */, int /* key */)
{
    std::string buf;
    buf.reserve(128);

    RL_SETSTATE(RL_STATE_MOREINPUT);
    SCOPE_EXIT(RL_UNSETSTATE(RL_STATE_MOREINPUT));
    int c;
    while ((c = rl_read_key()) >= 0)
    {
        if (c == '\r')
            c = '\n';
        buf.push_back(c);
        if (buf.size() >= BRACK_PASTE_SLEN && c == BRACK_PASTE_LAST && buf.substr(buf.size() - BRACK_PASTE_SLEN) == BRACK_PASTE_SUFF)
        {
            buf.resize(buf.size() - BRACK_PASTE_SLEN);
            break;
        }
    }
    trim(buf);
    return static_cast<size_t>(rl_insert_text(buf.c_str())) == buf.size() ? 0 : 1;
}

#endif

void ReadlineLineReader::enableBracketedPaste()
{
#if RL_VERSION_MAJOR >= 7
    rl_variable_bind("enable-bracketed-paste", "on");

    /// Use our bracketed paste handler to get better user experience. See comments above.
    rl_bind_keyseq(BRACK_PASTE_PREF, clickhouse_rl_bracketed_paste_begin);
#endif
};
