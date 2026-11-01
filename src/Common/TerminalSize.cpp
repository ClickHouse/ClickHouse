#include <cstdlib>
#include <string_view>
#include <unistd.h>
#include <sys/ioctl.h>
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

std::pair<uint16_t, uint16_t> getTerminalSize(int in_fd, int err_fd)
{
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
}

uint16_t getTerminalWidth(int in_fd, int err_fd)
{
    return getTerminalSize(in_fd, err_fd).first;
}

bool terminalSupportsUTF8()
{
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
