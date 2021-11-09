#include <base/setTerminalEcho.h>
#include <base/errnoToString.h>
#include <stdexcept>
#include <cstring>
#include <string>
#include <termios.h>
#include <unistd.h>


void setTerminalEcho(bool enable)
{
    /// Obtain terminal attributes,
    /// toggle the ECHO flag
    /// and set them back.

    struct termios tty{};

    if (0 != tcgetattr(STDIN_FILENO, &tty))
        throw std::runtime_error(std::string("setTerminalEcho failed get: ") + errnoToString(errno));

    if (enable)
        tty.c_lflag |= ECHO;
    else
        tty.c_lflag &= ~ECHO;

    if (0 != tcsetattr(STDIN_FILENO, TCSANOW, &tty))
        throw std::runtime_error(std::string("setTerminalEcho failed set: ") + errnoToString(errno));
}
