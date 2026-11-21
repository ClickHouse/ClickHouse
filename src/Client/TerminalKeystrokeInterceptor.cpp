#include <chrono>
#include <memory>
#include <Client/TerminalKeystrokeInterceptor.h>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#include <mutex>
#include <ostream>
#include <utility>
#include <termios.h>
#include <unistd.h>
#include <base/defines.h>
#include <sys/ioctl.h>
#ifdef __sun
#include <sys/filio.h>  // illumos defines FIONREAD in sys/filio.h, not sys/ioctl.h
#endif

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
    std::unique_lock<std::mutex> lock(mutex);

    if (intercept_thread && intercept_thread->joinable())
        return;

    {
        std::unique_lock<std::mutex> lk(stop_requested_mutex);
        stop_requested = false;
    }

    chassert(!orig_termios);

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
    if (tcsetattr(fd, TCSANOW, &raw))
        throw DB::ErrnoException(
            DB::ErrorCodes::SYSTEM_ERROR, "Cannot set terminal to the raw mode for the terminal referred to by file descriptor '{}'", fd);

    intercept_thread = std::make_unique<std::thread>(&TerminalKeystrokeInterceptor::run, this, callbacks);
}

void TerminalKeystrokeInterceptor::stopIntercept()
{
    std::unique_lock<std::mutex> lock(mutex);
    {
        std::unique_lock<std::mutex> lk(stop_requested_mutex);
        stop_requested = true;
    }
    stop_requested_cv.notify_all();

    if (intercept_thread && intercept_thread->joinable())
        intercept_thread->join();

    intercept_thread.reset();

    /// Set to the original (canonical) terminal mode.
    if (orig_termios)
    {
        if (tcsetattr(fd, TCSANOW, orig_termios.get()))
            throw DB::ErrnoException(
                DB::ErrorCodes::SYSTEM_ERROR,
                "Cannot set terminal to the original (canonical) mode for the terminal referred to by file descriptor '{}'",
                fd);

        orig_termios.reset();
    }
}

void TerminalKeystrokeInterceptor::run(TerminalKeystrokeInterceptor::CallbackMap map)
{
    constexpr auto intercept_interval_ms = std::chrono::milliseconds(200);
    std::unique_lock lock(stop_requested_mutex);
    while (!stop_requested)
    {
        try
        {
            runImpl(map);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        stop_requested_cv.wait_for(lock, intercept_interval_ms, [map, this] { return stop_requested; });
    }
}

std::string TerminalKeystrokeInterceptor::takeUnhandledInput()
{
    std::unique_lock<std::mutex> lk(stop_requested_mutex);
    return std::exchange(unhandled_input, {});
}

void TerminalKeystrokeInterceptor::runImpl(const DB::TerminalKeystrokeInterceptor::CallbackMap & map)
{
    int available = 0;
    if (ioctl(fd, FIONREAD, &available) < 0)
        throw DB::ErrnoException(DB::ErrorCodes::SYSTEM_ERROR, "ioctl({}, FIONREAD)", fd);

    if (available <= 0)
        return;

    /// Read everything available at once, so a multi-byte key is not split.
    std::string bytes(static_cast<size_t>(available), '\0');
    ssize_t res = read(fd, bytes.data(), bytes.size());  /// NOLINT(clang-analyzer-unix.BlockInCriticalSection)
    for (ssize_t i = 0; i < res; ++i)
    {
        /// A space typed after other text, or pasted along with it, is part of that text, not the hotkey.
        bool is_text = bytes[i] == ' ' && (!unhandled_input.empty() || res > 1);
        auto it = is_text ? map.end() : map.find(bytes[i]);
        if (it != map.end())
        {
            auto fn = it->second;
            fn();
        }
        else
            unhandled_input += bytes[i];
    }
}

}
