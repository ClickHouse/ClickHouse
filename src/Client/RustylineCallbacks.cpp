#include <Client/RustylineCallbacks.h>
#include <Client/RustylineCallbackContext.h>
#include <Client/RustylineLineReader.h>
#include <Client/ClientBaseHelpers.h>
#include <Common/UTF8Helpers.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>

#include <Common/quoteString.h>
#include <base/errnoToString.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>

#include "config.h"  // USE_SKIM

#if USE_SKIM
#   include <skim.h>
#endif

namespace DB::rustyline
{

namespace
{

/// Per-process state populated by RustylineLineReader at construction.
struct CallbackState
{
    LineReader::Suggest * suggest = nullptr;
    const Context * context = nullptr;
    const char * word_break_characters = "";
    bool rainbow_parentheses = true;
    bool embedded_mode = false;
};

CallbackState & state()
{
    static CallbackState s;
    return s;
}

std::string getEditorBinary()
{
    const char * editor = std::getenv("EDITOR"); // NOLINT(concurrency-mt-unsafe)
    if (!editor || !*editor)
        editor = "vim";
    return editor;
}

/// See ReplxxLineReader.cpp for the vfork-via-dlsym rationale.
int executeCommand(char * const argv[])
{
#if !defined(USE_MUSL)
    static void * real_vfork = dlsym(RTLD_DEFAULT, "vfork");
#else
    static void * real_vfork = reinterpret_cast<void *>(&vfork);
#endif
    if (!real_vfork)
        throw std::runtime_error("Cannot find vfork symbol");

    pid_t pid = reinterpret_cast<pid_t (*)()>(real_vfork)();
    if (-1 == pid)
        throw std::runtime_error(fmt::format("Cannot vfork {}: {}", argv[0], errnoToString()));

    if (0 == pid)
    {
        sigset_t mask;
        sigemptyset(&mask);
        sigprocmask(0, nullptr, &mask); // NOLINT(concurrency-mt-unsafe)
        sigprocmask(SIG_UNBLOCK, &mask, nullptr); // NOLINT(concurrency-mt-unsafe)
        execvp(argv[0], argv);
        _exit(-1);
    }

    int status = 0;
    while (true)
    {
        int exited_pid = waitpid(pid, &status, 0);
        if (exited_pid != -1)
            break;
        if (errno == EINTR)
            continue;
        throw std::runtime_error(fmt::format("Cannot waitpid {}: {}", pid, errnoToString()));
    }

    if (WIFEXITED(status))
        return WEXITSTATUS(status);
    if (WIFSIGNALED(status))
        throw std::runtime_error(fmt::format("Child process was terminated by signal {}", WTERMSIG(status)));
    throw std::runtime_error("Child process did not exit normally");
}

void writeRetry(int fd, const std::string & data)
{
    size_t written = 0;
    while (written != data.size())
    {
        ssize_t res = ::write(fd, data.data() + written, data.size() - written);
        if ((-1 == res || 0 == res) && errno != EINTR)
            throw std::runtime_error(fmt::format("Cannot write to {}: {}", fd, errnoToString()));
        if (res > 0)
            written += static_cast<size_t>(res);
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

class TemporaryFile
{
public:
    explicit TemporaryFile(const char * pattern) : path(pattern)
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
        if (fd != -1)
            ::close(fd);
        ::unlink(path.c_str());
    }
    void write(const std::string & data) { writeRetry(fd, data); }
    void close()
    {
        if (fd != -1)
        {
            ::close(fd);
            fd = -1;
        }
    }
    std::string & getPath() { return path; }

private:
    std::string path;
    int fd = -1;
};

}

void setCallbackContext(LineReader::Suggest * suggest, const Context * context,
                       const char * wbc, bool rainbow_parentheses, bool embedded_mode)
{
    auto & s = state();
    s.suggest = suggest;
    s.context = context;
    s.word_break_characters = wbc;
    s.rainbow_parentheses = rainbow_parentheses;
    s.embedded_mode = embedded_mode;
}

::rust::String cb_highlight(const std::string & line, ::std::size_t pos)
{
    auto & s = state();
    if (!s.context)
        return ::rust::String(line);
    try
    {
        /// rustyline gives `pos` as a byte offset; `highlightAnsi` expects a
        /// code-point index (matching replxx's contract).
        int code_point_pos = 0;
        for (::std::size_t i = 0; i < pos && i < line.size(); )
        {
            i += ::DB::UTF8::seqLength(line[i]);
            ++code_point_pos;
        }
        std::string out = ::DB::highlightAnsi(line, *s.context, code_point_pos, s.rainbow_parentheses);
        return ::rust::String(out);
    }
    catch (...)
    {
        /// Never let highlighting bring down the prompt — fall back to raw.
        return ::rust::String(line);
    }
}

static ::std::size_t completionStart(const ::std::string & line, ::std::size_t pos, const char * wbc)
{
    ::std::size_t start = pos;
    while (start > 0)
    {
        char c = line[start - 1];
        if (std::strchr(wbc, c) != nullptr)
            break;
        --start;
    }
    return start;
}

::std::size_t cb_complete_start(const ::std::string & line, ::std::size_t pos)
{
    auto & s = state();
    return completionStart(line, pos, s.word_break_characters ? s.word_break_characters : "");
}

::rust::Vec<::rust::String> cb_complete_candidates(const ::std::string & line, ::std::size_t pos)
{
    ::rust::Vec<::rust::String> out;
    auto & s = state();
    if (!s.suggest)
        return out;

    const char * wbc = s.word_break_characters ? s.word_break_characters : "";
    auto start = completionStart(line, pos, wbc);
    std::string prefix = line.substr(start, pos - start);

    auto completions = s.suggest->getCompletions(prefix, prefix.size(), wbc);
    for (const auto & c : completions)
        out.push_back(::rust::String(c));
    return out;
}

::rust::String cb_open_editor(const std::string & buf, bool format_query)
{
    auto & s = state();
    if (s.embedded_mode)
        return ::rust::String(buf);

    try
    {
        std::string query = buf;
        if (format_query)
            query = ::DB::formatQuery(std::move(query));

        TemporaryFile editor_file("clickhouse_client_editor_XXXXXX.sql");
        editor_file.write(query);
        editor_file.close();

        std::string editor = getEditorBinary();
        char * const argv[] = {editor.data(), editor_file.getPath().data(), nullptr};
        int rc = executeCommand(argv);
        if (rc == EXIT_SUCCESS)
            return ::rust::String(readFile(editor_file.getPath()));
        return ::rust::String(buf);
    }
    catch (const std::exception &)
    {
        return ::rust::String(buf);
    }
}

::rust::String cb_skim(const std::string & prefix, const ::std::vector<::std::string> & words)
{
#if USE_SKIM
    /// Reuse the skim crate already in the build.
    /// (`skim()` is defined in rust/workspace/skim/include/skim.h)
    return ::skim(prefix, words);
#else
    (void)prefix;
    (void)words;
    throw std::runtime_error("skim is not enabled in this build");
#endif
}

}
