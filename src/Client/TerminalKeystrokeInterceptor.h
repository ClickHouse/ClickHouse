#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

struct termios;

namespace DB
{

class TerminalKeystrokeInterceptor
{
    using Callback = std::function<void()>;
    using CallbackMap = std::unordered_map<char, Callback>;

public:
    explicit TerminalKeystrokeInterceptor(int fd_);
    ~TerminalKeystrokeInterceptor();
    void registerCallback(char key, Callback cb);

    void startIntercept();
    void stopIntercept();

private:
    void run(CallbackMap);
    void runImpl(const CallbackMap &) const;

    const int fd;

    std::mutex mutex;

    CallbackMap callbacks;
    std::unique_ptr<std::thread> intercept_thread;
    std::unique_ptr<struct termios> orig_termios;

    std::atomic_bool stop_requested = false;
};

}
