#include <unistd.h>
#include <sys/ioctl.h>
#if defined(OS_SUNOS)
#  include <sys/termios.h>
#endif
#include <Common/Exception.h>
#include <Common/TerminalSize.h>
#include <boost/program_options.hpp>


namespace DB::ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

uint16_t getTerminalWidth(int in_fd, int err_fd)
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
    return terminal_size.ws_col;
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
