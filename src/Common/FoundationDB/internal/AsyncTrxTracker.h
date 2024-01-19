#pragma once
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <Common/FoundationDB/fdb_c_fwd.h>

/// TODO: We need more test for cancel token.
/// For now, there is a small simple tests in gtest_fdb_coro.cpp

namespace DB::FoundationDB
{
class AsyncTrxCancelToken
{
public:
    virtual ~AsyncTrxCancelToken() = default;
    virtual bool canceled() const = 0;
    virtual std::unique_ptr<AsyncTrxCancelToken> splitToken();

    void setCancelPoint(FDBFuture * f) { future = f; }
    void assertNotCanceled() const;
    void cancelFuture();

protected:
    std::atomic<FDBFuture *> future = nullptr;
};

/// AsyncTrxTracker waits for all tracked trxs to finish before destructing.
class AsyncTrxTracker
{
public:
    ~AsyncTrxTracker();

    // Cancel all tracked trx
    void gracefullyCancelAll(bool wait = true);

    class Token;
    std::unique_ptr<Token> newToken();

private:
    std::mutex mutex;
    std::condition_variable cv;

    std::unordered_set<Token *> tokens;
    std::atomic<bool> cancel_requested = false;

    void waitAllTokenFinished();

public:
    class Token : public AsyncTrxCancelToken
    {
    public:
        explicit Token(AsyncTrxTracker & tracker);
        ~Token() override;

        bool canceled() const override { return tracker.cancel_requested; }
        std::unique_ptr<AsyncTrxCancelToken> splitToken() override { return tracker.newToken(); }

    private:
        AsyncTrxTracker & tracker;

        friend class AsyncTrxTracker;
    };
};

/// AsyncTrxCancelSource provide a <source, token> pair. It can wait for one trx to finish.
class AsyncTrxCancelSource
{
public:
    ~AsyncTrxCancelSource();
    void cancel();

    class Token;

    // Get paired token. getToken is thread-unsafe.
    std::unique_ptr<Token> getToken();

private:
    std::atomic<bool> cancel_requested = false;

    Token * token = nullptr;
    std::mutex token_mutex;
    std::condition_variable token_cv;

public:
    class Token : public AsyncTrxCancelToken
    {
    public:
        explicit Token(AsyncTrxCancelSource & tracker_);
        ~Token() override;

        bool canceled() const override { return tracker.cancel_requested; }

    private:
        AsyncTrxCancelSource & tracker;
    };
};
}
