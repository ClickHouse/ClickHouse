#include <cstdio>
#include <cstdlib>
#include <string_view>
#if defined(OS_WINDOWS)
#  include <io.h>
#  include <Poco/UnWindows.h>
#else
#  include <unistd.h>
#  include <sys/ioctl.h>
#endif
#if defined(OS_SUNOS)
#  include <sys/termios.h>
#endif
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/TerminalSize.h>
#include <boost/program_options.hpp>


namespace DB::ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

/// `in_fd` is used on POSIX only: see the comment on the Windows branch below.
std::pair<uint16_t, uint16_t> getTerminalSize([[maybe_unused]] int in_fd, int err_fd)
{
#if defined(OS_WINDOWS)
    /// Windows has no `TIOCGWINSZ`; the size is in the console's screen-buffer info. Take it
    /// from `srWindow`, the visible window, and not from `dwSize`, the screen buffer - the
    /// latter includes the scrollback and is typically far taller than the terminal.
    ///
    /// Only an *output* handle has a screen buffer, so `in_fd` is of no use here: unlike
    /// `ioctl(TIOCGWINSZ)`, which answers on any descriptor of the terminal, its Windows
    /// counterpart fails with `ERROR_INVALID_HANDLE` on the console *input* handle. Standard
    /// output is tried alongside `err_fd` so that the size is still found when only one of the
    /// two is redirected.
    ///
    /// `GetConsoleScreenBufferInfo` is also the predicate, rather than `_isatty` guarding it:
    /// `_isatty` is true for every character device, `NUL` and the console input handle
    /// included, so it cannot decide whether a descriptor has a screen buffer to measure -
    /// whereas `GetConsoleScreenBufferInfo` succeeds exactly on one that does. Its failure is
    /// therefore the answer "no console here", the same one the POSIX branch below gives for a
    /// descriptor that is not a tty, and not an error: `clickhouse.exe --version` with its
    /// output redirected to a pipe must print the version rather than throw.
    for (int fd : {err_fd, _fileno(stdout)})
    {
        auto * handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
        if (handle == INVALID_HANDLE_VALUE)
            continue;

        CONSOLE_SCREEN_BUFFER_INFO info{};
        if (!GetConsoleScreenBufferInfo(handle, &info))
            continue;

        return {static_cast<uint16_t>(info.srWindow.Right - info.srWindow.Left + 1),
                static_cast<uint16_t>(info.srWindow.Bottom - info.srWindow.Top + 1)};
    }

    /// Default - 0, as below.
    return {0, 0};
#else
    struct winsize terminal_size {};
    if (isatty(in_fd))
    {
        if (ioctl(in_fd, TIOCGWINSZ, &terminal_size))
            throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "Cannot obtain terminal window size (ioctl TIOCGWINSZ)");
    }
    else if (isatty(err_fd))
    {
        if (ioctl(err_fd, TIOCGWINSZ, &terminal_size))
            throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "Cannot obtain terminal window size (ioctl TIOCGWINSZ)");
    }
    /// Default - 0.
    return {terminal_size.ws_col, terminal_size.ws_row};
#endif
}

uint16_t getTerminalWidth(int in_fd, int err_fd)
{
    return getTerminalSize(in_fd, err_fd).first;
}

bool terminalSupportsUTF8()
{
#if defined(OS_WINDOWS)
    /// Windows does not take the console encoding from the locale environment variables - they
    /// are normally not set at all there - but from the console's output code page, which is
    /// what actually governs how the bytes we write are interpreted.
    return GetConsoleOutputCP() == CP_UTF8;
#else
    /// The character encoding is determined by the locale environment variables,
    /// in order of precedence: LC_ALL, LC_CTYPE, LANG.
    const char * locale = nullptr;
    for (const char * name : {"LC_ALL", "LC_CTYPE", "LANG"})
    {
        const char * value = std::getenv(name); /// NOLINT(concurrency-mt-unsafe)
        if (value && *value)
        {
            locale = value;
            break;
        }
    }

    /// If no locale is set, the default "C"/"POSIX" locale is in effect, which is not UTF-8.
    if (!locale)
        return false;

    /// Look for the substring "UTF" (case-insensitively), as in "en_US.UTF-8" or "C.utf8".
    /// No standard locale or encoding name contains these letters except for UTF encodings.
    std::string_view value(locale);
    for (size_t i = 0; i + 3 <= value.size(); ++i)
    {
        if ((value[i] == 'U' || value[i] == 'u')
            && (value[i + 1] == 'T' || value[i + 1] == 't')
            && (value[i + 2] == 'F' || value[i + 2] == 'f'))
            return true;
    }

    return false;
#endif
}

po::options_description createOptionsDescription(const std::string & caption, uint16_t terminal_width)
{
    unsigned line_length = po::options_description::m_default_line_length;
    unsigned min_description_length = line_length / 2;
    std::string longest_option_desc = "--http_native_compression_disable_checksumming_on_decompress";

    line_length = std::max(static_cast<uint16_t>(longest_option_desc.size()), terminal_width);
    min_description_length = std::min(min_description_length, line_length - 2);

    return po::options_description(caption, line_length, min_description_length);
}
