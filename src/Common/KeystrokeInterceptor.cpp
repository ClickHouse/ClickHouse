#include <chrono>
#include <memory>
#include <Common/KeystrokeInterceptor.h>

#include <termios.h>
#include <unistd.h>
#include <base/defines.h>

namespace DB
{

KeystrokeInterceptor::KeystrokeInterceptor(int fd_) : fd(fd_)
{
}

KeystrokeInterceptor::~KeystrokeInterceptor()
{
    stopIntercept();
}

void KeystrokeInterceptor::registerCallback(char key, KeystrokeInterceptor::Callback cb)
{
    callbacks.emplace(key, cb);
}

void KeystrokeInterceptor::startIntercept()
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

    intercept_thread = std::make_unique<std::thread>(&KeystrokeInterceptor::run, this, callbacks);
}

void KeystrokeInterceptor::stopIntercept()
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

void KeystrokeInterceptor::run(KeystrokeInterceptor::CallbackMap map)
{
    while (!stop_requested)
    {
        runImpl(map);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void KeystrokeInterceptor::runImpl(const DB::KeystrokeInterceptor::CallbackMap & map) const
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
