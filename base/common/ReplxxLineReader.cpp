#include <common/ReplxxLineReader.h>
#include <common/errnoToString.h>

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
#include <fmt/format.h>


namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

/// Copied from replxx::src/util.cxx::now_ms_str() under the terms of 3-clause BSD license of Replxx.
/// Copyright (c) 2017-2018, Marcin Konarski (amok at codestation.org)
/// Copyright (c) 2010, Salvatore Sanfilippo (antirez at gmail dot com)
/// Copyright (c) 2010, Pieter Noordhuis (pcnoordhuis at gmail dot com)
std::string replxx_now_ms_str()
{
    std::chrono::milliseconds ms(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()));
    time_t t = ms.count() / 1000;
    tm broken;
    if (!localtime_r(&t, &broken))
    {
        return std::string();
    }

    static int const BUFF_SIZE(32);
    char str[BUFF_SIZE];
    strftime(str, BUFF_SIZE, "%Y-%m-%d %H:%M:%S.", &broken);
    snprintf(str + sizeof("YYYY-mm-dd HH:MM:SS"), 5, "%03d", static_cast<int>(ms.count() % 1000));
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
            path.c_str(), errnoToString(errno).c_str());
        return;
    }

    std::string line;
    if (getline(in, line).bad())
    {
        rx.print("Cannot read from %s (for conversion): %s\n",
            path.c_str(), errnoToString(errno).c_str());
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
            path.c_str(), errnoToString(errno).c_str());
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

ReplxxLineReader::ReplxxLineReader(
    const Suggest & suggest,
    const String & history_file_path_,
    bool multiline_,
    Patterns extenders_,
    Patterns delimiters_,
    replxx::Replxx::highlighter_callback_t highlighter_)
    : LineReader(history_file_path_, multiline_, std::move(extenders_), std::move(delimiters_)), highlighter(std::move(highlighter_))
{
    using namespace std::placeholders;
    using Replxx = replxx::Replxx;

    if (!history_file_path.empty())
    {
        history_file_fd = open(history_file_path.c_str(), O_RDWR);
        if (history_file_fd < 0)
        {
            rx.print("Open of history file failed: %s\n", errnoToString(errno).c_str());
        }
        else
        {
            convertHistoryFile(history_file_path, rx);

            if (flock(history_file_fd, LOCK_SH))
            {
                rx.print("Shared lock of history file failed: %s\n", errnoToString(errno).c_str());
            }
            else
            {
                if (!rx.history_load(history_file_path))
                {
                    rx.print("Loading history failed: %s\n", errnoToString(errno).c_str());
                }

                if (flock(history_file_fd, LOCK_UN))
                {
                    rx.print("Unlock of history file failed: %s\n", errnoToString(errno).c_str());
                }
            }
        }
    }

    rx.install_window_change_handler();

    auto callback = [&suggest] (const String & context, size_t context_size)
    {
        if (auto range = suggest.getCompletions(context, context_size))
            return Replxx::completions_t(range->first, range->second);
        return Replxx::completions_t();
    };

    rx.set_completion_callback(callback);
    rx.set_complete_on_empty(false);
    rx.set_word_break_characters(word_break_characters);

    if (highlighter)
        rx.set_highlighter_callback(highlighter);

    /// By default C-p/C-n binded to COMPLETE_NEXT/COMPLETE_PREV,
    /// bind C-p/C-n to history-previous/history-next like readline.
    rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_NEXT, code); });
    rx.bind_key(Replxx::KEY::control('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_PREVIOUS, code); });
    /// By default COMPLETE_NEXT/COMPLETE_PREV was binded to C-p/C-n, re-bind
    /// to M-P/M-N (that was used for HISTORY_COMMON_PREFIX_SEARCH before, but
    /// it also binded to M-p/M-n).
    rx.bind_key(Replxx::KEY::meta('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_NEXT, code); });
    rx.bind_key(Replxx::KEY::meta('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_PREVIOUS, code); });
    /// By default M-BACKSPACE is KILL_TO_WHITESPACE_ON_LEFT, while in readline it is backward-kill-word
    rx.bind_key(Replxx::KEY::meta(Replxx::KEY::BACKSPACE), [this](char32_t code) { return rx.invoke(Replxx::ACTION::KILL_TO_BEGINING_OF_WORD, code); });
    /// By default C-w is KILL_TO_BEGINING_OF_WORD, while in readline it is unix-word-rubout
    rx.bind_key(Replxx::KEY::control('W'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::KILL_TO_WHITESPACE_ON_LEFT, code); });

    rx.bind_key(Replxx::KEY::meta('E'), [this](char32_t) { openEditor(); return Replxx::ACTION_RESULT::CONTINUE; });
}

ReplxxLineReader::~ReplxxLineReader()
{
    if (close(history_file_fd))
        rx.print("Close of history file failed: %s\n", errnoToString(errno).c_str());
}

LineReader::InputStatus ReplxxLineReader::readOneLine(const String & prompt)
{
    input.clear();

    const char* cinput = rx.input(prompt);
    if (cinput == nullptr)
        return (errno != EAGAIN) ? ABORT : RESET_LINE;
    input = cinput;

    trim(input);
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
    if (flock(history_file_fd, LOCK_EX))
        rx.print("Lock of history file failed: %s\n", errnoToString(errno).c_str());
    else
        locked = true;

    rx.history_add(line);

    // flush changes to the disk
    if (!rx.history_save(history_file_path))
        rx.print("Saving history failed: %s\n", errnoToString(errno).c_str());

    if (locked && 0 != flock(history_file_fd, LOCK_UN))
        rx.print("Unlock of history file failed: %s\n", errnoToString(errno).c_str());
}

int ReplxxLineReader::execute(const std::string & command)
{
    std::vector<char> argv0("sh", &("sh"[3]));
    std::vector<char> argv1("-c", &("-c"[3]));
    std::vector<char> argv2(command.data(), command.data() + command.size() + 1);

    const char * filename = "/bin/sh";
    char * const argv[] = {argv0.data(), argv1.data(), argv2.data(), nullptr};

    static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");
    if (!real_vfork)
    {
        rx.print("Cannot find symbol vfork in myself: %s\n", errnoToString(errno).c_str());
        return -1;
    }

    pid_t pid = reinterpret_cast<pid_t (*)()>(real_vfork)();

    if (-1 == pid)
    {
        rx.print("Cannot vfork: %s\n", errnoToString(errno).c_str());
        return -1;
    }

    if (0 == pid)
    {
        sigset_t mask;
        sigemptyset(&mask);
        sigprocmask(0, nullptr, &mask);
        sigprocmask(SIG_UNBLOCK, &mask, nullptr);

        execv(filename, argv);
        _exit(-1);
    }

    int status = 0;
    if (-1 == waitpid(pid, &status, 0))
    {
        rx.print("Cannot waitpid: %s\n", errnoToString(errno).c_str());
        return -1;
    }
    return status;
}

void ReplxxLineReader::openEditor()
{
    char filename[] = "clickhouse_replxx_XXXXXX.sql";
    int fd = ::mkstemps(filename, 4);
    if (-1 == fd)
    {
        rx.print("Cannot create temporary file to edit query: %s\n", errnoToString(errno).c_str());
        return;
    }

    const char * editor = std::getenv("EDITOR");
    if (!editor || !*editor)
        editor = "vim";

    replxx::Replxx::State state(rx.get_state());

    size_t bytes_written = 0;
    const char * begin = state.text();
    size_t offset = strlen(state.text());
    while (bytes_written != offset)
    {
        ssize_t res = ::write(fd, begin + bytes_written, offset - bytes_written);
        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            rx.print("Cannot write to temporary query file %s: %s\n", filename, errnoToString(errno).c_str());
            break;
        }
        bytes_written += res;
    }

    if (0 != ::close(fd))
    {
        rx.print("Cannot close temporary query file %s: %s\n", filename, errnoToString(errno).c_str());
        return;
    }

    if (0 == execute(fmt::format("{} {}", editor, filename)))
    {
        try
        {
            std::ifstream t(filename);
            std::string str;
            t.seekg(0, std::ios::end);
            str.reserve(t.tellg());
            t.seekg(0, std::ios::beg);
            str.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
            rx.set_state(replxx::Replxx::State(str.c_str(), str.size()));
        }
        catch (...)
        {
            rx.print("Cannot read from temporary query file %s: %s\n", filename, errnoToString(errno).c_str());
            return;
        }
    }

    if (bracketed_paste_enabled)
        enableBracketedPaste();

    if (0 != ::unlink(filename))
        rx.print("Cannot remove temporary query file %s: %s\n", filename, errnoToString(errno).c_str());
}

void ReplxxLineReader::enableBracketedPaste()
{
    bracketed_paste_enabled = true;
    rx.enable_bracketed_paste();
};
