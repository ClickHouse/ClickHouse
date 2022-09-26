#include <base/ReplxxLineReader.h>
#include <base/errnoToString.h>

#include <stdexcept>
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
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp> /// is_any_of

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
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

std::string getFuzzyFinder()
{
    const char * env_path = std::getenv("PATH"); // NOLINT(concurrency-mt-unsafe)

    if (!env_path || !*env_path)
        return {};

    std::vector<std::string> paths;
    boost::split(paths, env_path, boost::is_any_of(":"));
    for (const auto & path_str : paths)
    {
        std::filesystem::path path(path_str);
        std::filesystem::path sk_bin_path = path / "sk";
        if (!access(sk_bin_path.c_str(), X_OK))
            return sk_bin_path;

        std::filesystem::path fzf_bin_path = path / "fzf";
        if (!access(fzf_bin_path.c_str(), X_OK))
            return fzf_bin_path;
    }

    return {};
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

    return status;
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
    std::ifstream t(path);
    std::string str;
    t.seekg(0, std::ios::end);
    str.reserve(t.tellg());
    t.seekg(0, std::ios::beg);
    str.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
    return str;
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
            fd = ::mkstemps(path.data(), path.size() - dot_pos);
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
            fmt::print(stderr, "{}", e.what());
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
    tm broken;
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

static bool replxx_last_is_delimiter = false;
void ReplxxLineReader::setLastIsDelimiter(bool flag)
{
    replxx_last_is_delimiter = flag;
}

ReplxxLineReader::ReplxxLineReader(
    Suggest & suggest,
    const String & history_file_path_,
    bool multiline_,
    Patterns extenders_,
    Patterns delimiters_,
    replxx::Replxx::highlighter_callback_t highlighter_)
    : LineReader(history_file_path_, multiline_, std::move(extenders_), std::move(delimiters_)), highlighter(std::move(highlighter_))
    , editor(getEditor())
    , fuzzy_finder(getFuzzyFinder())
{
    using namespace std::placeholders;
    using Replxx = replxx::Replxx;

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

    auto callback = [&suggest] (const String & context, size_t context_size)
    {
        return suggest.getCompletions(context, context_size);
    };

    rx.set_completion_callback(callback);
    rx.set_complete_on_empty(false);
    rx.set_word_break_characters(word_break_characters);
    rx.set_ignore_case(true);

    if (highlighter)
        rx.set_highlighter_callback(highlighter);

    /// By default C-p/C-n binded to COMPLETE_NEXT/COMPLETE_PREV,
    /// bind C-p/C-n to history-previous/history-next like readline.
    rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_NEXT, code); });
    rx.bind_key(Replxx::KEY::control('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_PREVIOUS, code); });

    auto commit_action = [this](char32_t code)
    {
        /// If we allow multiline and there is already something in the input, start a newline.
        /// NOTE: Lexer is only available if we use highlighter.
        if (highlighter && multiline && !replxx_last_is_delimiter)
            return rx.invoke(Replxx::ACTION::NEW_LINE, code);
        replxx_last_is_delimiter = false;
        return rx.invoke(Replxx::ACTION::COMMIT_LINE, code);
    };
    /// bind C-j to ENTER action.
    rx.bind_key(Replxx::KEY::control('J'), commit_action);
    rx.bind_key(Replxx::KEY::ENTER, commit_action);

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
        rx.set_state(replxx::Replxx::State(commented_line.c_str(), commented_line.size()));

        return rx.invoke(Replxx::ACTION::COMMIT_LINE, code);
    };
    rx.bind_key(Replxx::KEY::meta('#'), insert_comment_action);

    /// interactive search in history (requires fzf/sk)
    if (!fuzzy_finder.empty())
    {
        auto interactive_history_search = [this](char32_t code)
        {
            openInteractiveHistorySearch();
            return rx.invoke(Replxx::ACTION::REPAINT, code);
        };
        rx.bind_key(Replxx::KEY::control('R'), interactive_history_search);
    }
}

ReplxxLineReader::~ReplxxLineReader()
{
    if (close(history_file_fd))
        rx.print("Close of history file failed: %s\n", errnoToString().c_str());
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
        rx.print("Lock of history file failed: %s\n", errnoToString().c_str());
    else
        locked = true;

    rx.history_add(line);

    // flush changes to the disk
    if (!rx.history_save(history_file_path))
        rx.print("Saving history failed: %s\n", errnoToString().c_str());

    if (locked && 0 != flock(history_file_fd, LOCK_UN))
        rx.print("Unlock of history file failed: %s\n", errnoToString().c_str());
}

void ReplxxLineReader::openEditor()
{
    TemporaryFile editor_file("clickhouse_client_editor_XXXXXX.sql");
    editor_file.write(rx.get_state().text());
    editor_file.close();

    char * const argv[] = {editor.data(), editor_file.getPath().data(), nullptr};
    try
    {
        if (executeCommand(argv) == 0)
        {
            const std::string & new_query = readFile(editor_file.getPath());
            rx.set_state(replxx::Replxx::State(new_query.c_str(), new_query.size()));
        }
    }
    catch (const std::runtime_error & e)
    {
        rx.print(e.what());
    }

    if (bracketed_paste_enabled)
        enableBracketedPaste();
}

void ReplxxLineReader::openInteractiveHistorySearch()
{
    assert(!fuzzy_finder.empty());
    TemporaryFile history_file("clickhouse_client_history_in_XXXXXX.bin");
    auto hs(rx.history_scan());
    while (hs.next())
    {
        history_file.write(hs.get().text());
        history_file.write(std::string(1, '\0'));
    }
    history_file.close();

    TemporaryFile output_file("clickhouse_client_history_out_XXXXXX.sql");
    output_file.close();

    char sh[] = "sh";
    char sh_c[] = "-c";
    /// NOTE: You can use one of the following to configure the behaviour additionally:
    /// - SKIM_DEFAULT_OPTIONS
    /// - FZF_DEFAULT_OPTS
    std::string fuzzy_finder_command = fmt::format(
        "{} --read0 --tac --no-sort --tiebreak=index --bind=ctrl-r:toggle-sort --height=30% < {} > {}",
        fuzzy_finder, history_file.getPath(), output_file.getPath());
    char * const argv[] = {sh, sh_c, fuzzy_finder_command.data(), nullptr};

    try
    {
        if (executeCommand(argv) == 0)
        {
            const std::string & new_query = readFile(output_file.getPath());
            rx.set_state(replxx::Replxx::State(new_query.c_str(), new_query.size()));
        }
    }
    catch (const std::runtime_error & e)
    {
        rx.print(e.what());
    }

    if (bracketed_paste_enabled)
        enableBracketedPaste();
}

void ReplxxLineReader::enableBracketedPaste()
{
    bracketed_paste_enabled = true;
    rx.enable_bracketed_paste();
}
