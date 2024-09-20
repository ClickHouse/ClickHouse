#include <chrono>
#include <memory>
#include <Client/TerminalKeystrokeInterceptor.h>

#include <Common/Exception.h>

#include <ostream>
#include <termios.h>
#include <unistd.h>
#include <base/defines.h>

namespace DB::ErrorCodes
{
extern const int SYSTEM_ERROR;
}

namespace DB
{

TerminalKeystrokeInterceptor::TerminalKeystrokeInterceptor(int fd_, std::ostream & error_stream_) : fd(fd_), error_stream(error_stream_)
{
}

TerminalKeystrokeInterceptor::~TerminalKeystrokeInterceptor()
{
    try
    {
        stopIntercept();
    }
    catch (...)
    {
        error_stream << getCurrentExceptionMessage(false);
    }
}

void TerminalKeystrokeInterceptor::registerCallback(char key, TerminalKeystrokeInterceptor::Callback cb)
{
    callbacks.emplace(key, cb);
}

void TerminalKeystrokeInterceptor::startIntercept()
{
    std::lock_guard<std::mutex> lock(mutex);

    if (intercept_thread && intercept_thread->joinable())
        return;

    chassert(!orig_termios);

    stop_requested = false;

    /// Save terminal state.
    orig_termios = std::make_unique<struct termios>();
    if (tcgetattr(fd, orig_termios.get()))
        throw DB::ErrnoException(
            DB::ErrorCodes::SYSTEM_ERROR, "Cannot get the state of the terminal referred to by file descriptor '{}'", fd);

    /// Set terminal to the raw terminal mode.
    struct termios raw = *orig_termios;
    raw.c_lflag &= ~(ECHO | ICANON);
    raw.c_cc[VMIN] = 0;
    raw.c_cc[VTIME] = 1;
    if (tcsetattr(fd, TCSAFLUSH, &raw))
        throw DB::ErrnoException(
            DB::ErrorCodes::SYSTEM_ERROR, "Cannot set terminal to the raw mode for the terminal referred to by file descriptor '{}'", fd);

    intercept_thread = std::make_unique<std::thread>(&TerminalKeystrokeInterceptor::run, this, callbacks);
}

void TerminalKeystrokeInterceptor::stopIntercept()
{
    stop_requested = true;

    std::lock_guard<std::mutex> lock(mutex);

    if (intercept_thread && intercept_thread->joinable())
    {
        intercept_thread->join();
        intercept_thread.reset();
    }

    /// Set to the original (canonical) terminal mode.
    if (orig_termios)
    {
        if (tcsetattr(fd, TCSAFLUSH, orig_termios.get()))
            throw DB::ErrnoException(
                DB::ErrorCodes::SYSTEM_ERROR,
                "Cannot set terminal to the original (canonical) mode for the terminal referred to by file descriptor '{}'",
                fd);

        orig_termios.reset();
    }
}

void TerminalKeystrokeInterceptor::run(TerminalKeystrokeInterceptor::CallbackMap map)
{
    while (!stop_requested)
    {
        runImpl(map);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void TerminalKeystrokeInterceptor::runImpl(const DB::TerminalKeystrokeInterceptor::CallbackMap & map) const
{
    char ch;
    if (read(fd, &ch, 1) > 0)
    {
        auto it = map.find(ch);
        if (it != map.end())
        {
            auto fn = it->second;
            fn();
        }
    }
}

}
