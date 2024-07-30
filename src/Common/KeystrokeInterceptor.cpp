#include <chrono>
#include <memory>
#include <Common/KeystrokeInterceptor.h>

#include <fcntl.h>
#include <termios.h>
#include <unistd.h>

namespace DB
{

KeystrokeInterceptor::KeystrokeInterceptor(int fd_) : fd(fd_), orig_termios(std::make_unique<struct termios>())
{
    /// TODO: process errors.
    tcgetattr(fd, orig_termios.get());
}

KeystrokeInterceptor::~KeystrokeInterceptor()
{
    stopIntercept();
}

void KeystrokeInterceptor::registerCallback(char ch, KeystrokeInterceptor::Callback cb)
{
    callbacks.emplace(ch, cb);
}

void KeystrokeInterceptor::startIntercept()
{
    std::lock_guard<std::mutex> lock(mutex);

    if (auto running = intercept_thread && intercept_thread->joinable())
        return;

    /// Save terminal state.
    /// TODO: process errors.
    tcgetattr(fd, orig_termios.get());

    /// Enable raw terminal mode.
    struct termios raw = *orig_termios;
    raw.c_lflag &= ~(ECHO | ICANON);
    raw.c_cc[VMIN] = 0;
    raw.c_cc[VTIME] = 1;
    tcsetattr(fd, TCSAFLUSH, &raw);

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    intercept_thread = std::make_unique<std::thread>(&KeystrokeInterceptor::run, this, callbacks);
}

void KeystrokeInterceptor::stopIntercept()
{
    stop_requested = true;

    std::lock_guard<std::mutex> lock(mutex);

    if (auto running = intercept_thread && intercept_thread->joinable())
    {
        intercept_thread->join();
        intercept_thread.reset();
    }

    /// TODO: process errors.
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | ~O_NONBLOCK);

    /// Reset original terminal mode.
    tcsetattr(fd, TCSAFLUSH, orig_termios.get());
}

void KeystrokeInterceptor::run(KeystrokeInterceptor::CallbackMap map)
{
    while (!stop_requested)
    {
        runImpl(map);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void KeystrokeInterceptor::runImpl(const DB::KeystrokeInterceptor::CallbackMap & map)
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
