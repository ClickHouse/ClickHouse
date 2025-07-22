#pragma once
#include <memory>
#include <functional>
#include <list>
#include <atomic>

/// Just like std::stop_token, which isn't available yet. A.k.a. folly::CancellationToken.
/// When we switch to C++20, delete this and use std::stop_token instead.

struct StopState;
using StopStatePtr = std::shared_ptr<StopState>;

class StopToken
{
public:
    StopToken() = default;

    StopToken(const StopToken &) = default;
    StopToken(StopToken &&) = default;
    StopToken & operator=(const StopToken &) = default;
    StopToken & operator=(StopToken &&) = default;

    bool stop_requested() const;
    bool stop_possible() const { return state != nullptr; }

private:
    friend class StopSource;
    friend class StopCallback;

    StopStatePtr state;

    explicit StopToken(StopStatePtr s) : state(std::move(s)) {}
};

class StopSource
{
public:
    StopSource();

    StopSource(const StopSource &) = default;
    StopSource(StopSource &&) = default;
    StopSource & operator=(const StopSource &) = default;
    StopSource & operator=(StopSource &&) = default;

    StopToken get_token() const { return StopToken(state); }
    bool request_stop();

private:
    StopStatePtr state;
};

class StopCallback
{
public:
    using Callback = std::function<void()>;

    StopCallback(const StopToken & token, Callback cb);
    /// If the callback is already running, waits for it to return.
    ~StopCallback();

    StopCallback(const StopCallback &) = delete;
    StopCallback & operator=(const StopCallback &) = delete;

private:
    friend class StopSource;

    StopStatePtr state;
    std::list<StopCallback *>::iterator it;
    Callback callback;
    std::atomic_bool returned {false};
};
