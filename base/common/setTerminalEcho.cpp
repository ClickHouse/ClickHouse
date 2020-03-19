// https://stackoverflow.com/questions/1413445/reading-a-password-from-stdcin

#include "setTerminalEcho.h"

#include <cstring>
#include <stdexcept>
#include <string>

#include <errno.h>
#include <termios.h>
#include <unistd.h>

void setTerminalEcho(bool enable)
{
    struct termios tty;
    if (tcgetattr(STDIN_FILENO, &tty))
        throw std::runtime_error(std::string("setTerminalEcho failed get: ") + strerror(errno));
    if (!enable)
        tty.c_lflag &= ~ECHO;
    else
        tty.c_lflag |= ECHO;

    auto ret = tcsetattr(STDIN_FILENO, TCSANOW, &tty);
    if (ret)
        throw std::runtime_error(std::string("setTerminalEcho failed set: ") + strerror(errno));
}
