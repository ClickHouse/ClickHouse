#include <common/ReplxxLineReader.h>
#include <common/errnoToString.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <functional>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <csignal>
#include <dlfcn.h>
#include <fcntl.h>
#include <fstream>

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
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

    String editor = std::getenv("EDITOR");
    if (editor.empty())
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
            return;
        }
        bytes_written += res;
    }

    if (0 != ::close(fd))
    {
        rx.print("Cannot close temporary query file %s: %s\n", filename, errnoToString(errno).c_str());
        return;
    }

    if (0 == execute(editor + " " + filename))
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
