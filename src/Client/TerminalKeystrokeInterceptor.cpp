#include <chrono>
#include <memory>
#include <Client/TerminalKeystrokeInterceptor.h>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#include <mutex>
#include <ostream>
#include <base/defines.h>

#if defined(OS_WINDOWS)
#include <io.h>
#include <Poco/UnWindows.h>
#else
#include <termios.h>
#include <unistd.h>
#include <sys/ioctl.h>
#ifdef __sun
#include <sys/filio.h>  // illumos defines FIONREAD in sys/filio.h, not sys/ioctl.h
#endif
#endif

namespace DB::ErrorCodes
{
extern const int SYSTEM_ERROR;
}

namespace DB
{

#if defined(OS_WINDOWS)

/// The console input mode. Its two relevant flags are the counterparts of `ICANON` and `ECHO`:
/// with `ENABLE_LINE_INPUT` set the console hands over input only when Enter is pressed, and
/// `ENABLE_ECHO_INPUT` prints what is typed.
///
/// `ENABLE_PROCESSED_INPUT` - the counterpart of `ISIG` - is deliberately left on, exactly as the
/// POSIX `enterRawMode` below leaves `ISIG` on: Ctrl+C keeps arriving as a console control event
/// for the handler installed by `ClientApplicationBase::setupSignalHandler`, rather than as a key
/// event that no registered callback would consume. The embedded client, whose `0x03` callback
/// does expect Ctrl+C as ordinary input, gets its input over an SSH channel where the byte is
/// part of the stream and never passes through a Windows console.
struct TerminalState
{
    DWORD console_mode = 0;
};

namespace
{

HANDLE toConsoleHandle(int fd)
{
    auto * handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (handle == INVALID_HANDLE_VALUE)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "File descriptor {} is not a valid handle", fd);
    return handle;
}

std::unique_ptr<TerminalState> enterRawMode(int fd)
{
    auto * handle = toConsoleHandle(fd);

    auto state = std::make_unique<TerminalState>();
    if (!GetConsoleMode(handle, &state->console_mode))
        throw Exception(
            ErrorCodes::SYSTEM_ERROR, "Cannot get the state of the terminal referred to by file descriptor '{}'", fd);

    if (!SetConsoleMode(handle, state->console_mode & ~static_cast<DWORD>(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT)))
        throw Exception(
            ErrorCodes::SYSTEM_ERROR, "Cannot set terminal to the raw mode for the terminal referred to by file descriptor '{}'", fd);

    return state;
}

void restoreMode(int fd, const TerminalState & state)
{
    if (!SetConsoleMode(toConsoleHandle(fd), state.console_mode))
        throw Exception(
            ErrorCodes::SYSTEM_ERROR,
            "Cannot set terminal to the original (canonical) mode for the terminal referred to by file descriptor '{}'",
            fd);
}

/// Reads one character if one is waiting, without blocking. Returns whether it read one.
///
/// There is no `FIONREAD` for a console. `GetNumberOfConsoleInputEvents` counts input *records*,
/// most of which carry no character at all - key releases, focus changes, window resizes, mouse
/// movement - so a plain `ReadFile` would block on the first of those. Consume records until one
/// turns out to be a key press.
bool readKeystroke(int fd, char & ch)
{
    auto * handle = toConsoleHandle(fd);

    DWORD pending = 0;
    if (!GetNumberOfConsoleInputEvents(handle, &pending))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot get the number of pending console input events");

    for (; pending > 0; --pending)
    {
        INPUT_RECORD record{};
        DWORD read = 0;
        if (!ReadConsoleInputW(handle, &record, 1, &read) || read != 1)
            return false;

        if (record.EventType != KEY_EVENT || !record.Event.KeyEvent.bKeyDown)
            continue;

        /// The callbacks are keyed by `char`, so only the characters that fit are of interest;
        /// anything else - a function key, or a character outside ASCII - has no callback.
        const auto character = record.Event.KeyEvent.uChar.UnicodeChar;
        if (character == 0 || character > 0x7F)
            continue;

        ch = static_cast<char>(character);
        return true;
    }

    return false;
}

}

#else

struct TerminalState
{
    termios attributes{};
};

namespace
{

std::unique_ptr<TerminalState> enterRawMode(int fd)
{
    auto state = std::make_unique<TerminalState>();
    if (tcgetattr(fd, &state->attributes))
        throw ErrnoException(
            ErrorCodes::SYSTEM_ERROR, "Cannot get the state of the terminal referred to by file descriptor '{}'", fd);

    termios raw = state->attributes;
    raw.c_lflag &= ~(ECHO | ICANON);
    raw.c_cc[VMIN] = 0;
    raw.c_cc[VTIME] = 1;
    if (tcsetattr(fd, TCSAFLUSH, &raw))
        throw ErrnoException(
            ErrorCodes::SYSTEM_ERROR, "Cannot set terminal to the raw mode for the terminal referred to by file descriptor '{}'", fd);

    return state;
}

void restoreMode(int fd, const TerminalState & state)
{
    if (tcsetattr(fd, TCSAFLUSH, &state.attributes))
        throw ErrnoException(
            ErrorCodes::SYSTEM_ERROR,
            "Cannot set terminal to the original (canonical) mode for the terminal referred to by file descriptor '{}'",
            fd);
}

bool readKeystroke(int fd, char & ch)
{
    int available = 0;
    if (ioctl(fd, FIONREAD, &available) < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "ioctl({}, FIONREAD)", fd);

    if (available <= 0)
        return false;

    return read(fd, &ch, 1) > 0;  /// NOLINT(clang-analyzer-unix.BlockInCriticalSection)
}

}

#endif

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

    chassert(!orig_terminal_state);

    orig_terminal_state = enterRawMode(fd);

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

    if (orig_terminal_state)
    {
        restoreMode(fd, *orig_terminal_state);
        orig_terminal_state.reset();
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

void TerminalKeystrokeInterceptor::runImpl(const DB::TerminalKeystrokeInterceptor::CallbackMap & map) const
{
    char ch = 0;
    if (readKeystroke(fd, ch))
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
