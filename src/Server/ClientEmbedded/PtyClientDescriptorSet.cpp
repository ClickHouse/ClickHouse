#include <Server/ClientEmbedded/PtyClientDescriptorSet.h>
#include <Common/Exception.h>
#include "openpty.h"

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
            ErrnoException(ErrorCodes::SYSTEM_ERROR, "Unexpected error while closing file descriptor");
    }
    fd = -1;
}


PtyClientDescriptorSet::PtyClientDescriptorSet(const String & term_name_, int width, int height, int width_pixels, int height_pixels)
    : term_name(term_name_)
{
    winsize winsize{};
    winsize.ws_col = width;
    winsize.ws_row = height;
    winsize.ws_xpixel = width_pixels;
    winsize.ws_ypixel = height_pixels;
    int pty_master_raw = -1, pty_slave_raw = -1;
    if (openpty(&pty_master_raw, &pty_slave_raw, nullptr, nullptr, &winsize) != 0)
    {
        ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot open pty");
    }
    pty_master.capture(pty_master_raw);
    pty_slave.capture(pty_slave_raw);
    fd_source.open(pty_slave.get(), boost::iostreams::never_close_handle);
    fd_sink.open(pty_slave.get(), boost::iostreams::never_close_handle);

    // disable signals from tty
    struct termios tios;
    if (tcgetattr(pty_slave.get(), &tios) == -1)
    {
        ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot get termios from tty via tcgetattr");
    }
    tios.c_lflag &= ~ISIG;
    if (tcsetattr(pty_slave.get(), TCSANOW, &tios) == -1)
    {
        ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot set termios to tty via tcsetattr");
    }
    input_stream.open(fd_source);
    output_stream.open(fd_sink);
    output_stream << std::unitbuf;
}


void PtyClientDescriptorSet::changeWindowSize(int width, int height, int width_pixels, int height_pixels) const
{
    winsize winsize{};
    winsize.ws_col = width;
    winsize.ws_row = height;
    winsize.ws_xpixel = width_pixels;
    winsize.ws_ypixel = height_pixels;

    if (ioctl(pty_master.get(), TIOCSWINSZ, &winsize) == -1)
    {
        ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot update terminal window size via ioctl TIOCSWINSZ");
    }
}


PtyClientDescriptorSet::~PtyClientDescriptorSet() = default;

}
