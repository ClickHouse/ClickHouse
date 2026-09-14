#if defined(OS_LINUX)

#include <Server/ClientEmbedded/PtyClientDescriptorSet.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#include <base/openpty.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

void PtyClientDescriptorSet::FileDescriptorWrapper::close()
{
    if (fd != -1)
    {
        if (::close(fd) != 0 && errno != EINTR)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Unexpected error while closing file descriptor");
    }
    fd = -1;
}


PtyClientDescriptorSet::PtyClientDescriptorSet(const String & term_name_, int width, int height, int width_pixels, int height_pixels)
    : term_name(term_name_)
{
    winsize winsize{};
    winsize.ws_col = static_cast<uint16_t>(width);
    winsize.ws_row = static_cast<uint16_t>(height);
    winsize.ws_xpixel = static_cast<uint16_t>(width_pixels);
    winsize.ws_ypixel = static_cast<uint16_t>(height_pixels);
    int pty_master_raw = -1;
    int pty_slave_raw = -1;
    if (openpty(&pty_master_raw, &pty_slave_raw, nullptr, nullptr, &winsize) != 0)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open pty");
    }
    pty_master.capture(pty_master_raw);
    pty_slave.capture(pty_slave_raw);
    fd_source.open(pty_slave.get(), boost::iostreams::never_close_handle);
    fd_sink.open(pty_slave.get(), boost::iostreams::never_close_handle);

    // disable signals from tty
    struct termios tios;
    if (tcgetattr(pty_slave.get(), &tios) == -1)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot get termios from tty via tcgetattr");
    }
    tios.c_lflag &= ~ISIG;
    if (tcsetattr(pty_slave.get(), TCSANOW, &tios) == -1)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set termios to tty via tcsetattr");
    }
    input_stream.open(fd_source);
    output_stream.open(fd_sink);
    output_stream << std::unitbuf;
}


void PtyClientDescriptorSet::changeWindowSize(int width, int height, int width_pixels, int height_pixels) const
{
    winsize winsize{};
    winsize.ws_col = static_cast<uint16_t>(width);
    winsize.ws_row = static_cast<uint16_t>(height);
    winsize.ws_xpixel = static_cast<uint16_t>(width_pixels);
    winsize.ws_ypixel = static_cast<uint16_t>(height_pixels);

    if (ioctl(pty_master.get(), TIOCSWINSZ, &winsize) == -1)
    {
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot update terminal window size via ioctl TIOCSWINSZ");
    }
}


PtyClientDescriptorSet::~PtyClientDescriptorSet() = default;

}

#endif
