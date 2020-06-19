// https://stackoverflow.com/questions/1413445/reading-a-password-from-stdcin

#include "setTerminalEcho.h"

#include <cstring>
#include <stdexcept>
#include <string>

#ifdef WIN32
#    include <windows.h>
#else
#    include <errno.h>
#    include <termios.h>
#    include <unistd.h>
#endif

void setTerminalEcho(bool enable)
{
#if defined(WIN32)
    auto handle = GetStdHandle(STD_INPUT_HANDLE);
    DWORD mode;
    if (!GetConsoleMode(handle, &mode))
        throw std::runtime_error(std::string("setTerminalEcho failed get: ") + std::to_string(GetLastError()));

    if (!enable)
        mode &= ~ENABLE_ECHO_INPUT;
    else
        mode |= ENABLE_ECHO_INPUT;

    if (!SetConsoleMode(handle, mode))
        throw std::runtime_error(std::string("setTerminalEcho failed set: ") + std::to_string(GetLastError()));
#else
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
#endif
}
