#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <thread>
#include <unordered_map>

namespace DB
{

/// The terminal settings this interceptor replaced, to be put back when it stops. What they are
/// differs by platform - a `termios` on POSIX, a console mode word on Windows - and neither
/// belongs in a header, so the type is opaque here and defined in the implementation.
struct TerminalState;

class TerminalKeystrokeInterceptor
{
    using Callback = std::function<void()>;
    using CallbackMap = std::unordered_map<char, Callback>;

public:
    explicit TerminalKeystrokeInterceptor(int fd_, std::ostream & error_stream_);
    ~TerminalKeystrokeInterceptor();
    void registerCallback(char key, Callback cb);

    void startIntercept();
    void stopIntercept();

private:
    void run(CallbackMap);
    void runImpl(const CallbackMap &) const;

    const int fd;
    std::ostream & error_stream;

    std::mutex mutex;

    CallbackMap callbacks;
    std::unique_ptr<std::thread> intercept_thread;
    std::unique_ptr<TerminalState> orig_terminal_state;

    bool stop_requested = false;
    std::mutex stop_requested_mutex;
    std::condition_variable stop_requested_cv;
};

}
