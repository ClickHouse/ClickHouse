#include <chrono>
#include <memory>
#include <Client/TerminalKeystrokeInterceptor.h>

#include <termios.h>
#include <unistd.h>
#include <base/defines.h>

namespace DB
{

TerminalKeystrokeInterceptor::TerminalKeystrokeInterceptor(int fd_) : fd(fd_)
{
}

TerminalKeystrokeInterceptor::~TerminalKeystrokeInterceptor()
{
    stopIntercept();
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
    tcgetattr(fd, orig_termios.get());

    /// Enable raw terminal mode.
    struct termios raw = *orig_termios;
    raw.c_lflag &= ~(ECHO | ICANON);
    raw.c_cc[VMIN] = 0;
    raw.c_cc[VTIME] = 1;
    tcsetattr(fd, TCSAFLUSH, &raw);

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

    /// Reset original terminal mode.
    if (orig_termios)
    {
        tcsetattr(fd, TCSAFLUSH, orig_termios.get());
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
