#include <memory>
#include <mutex>
#include <foundationdb/fdb_c.h>
#include <Poco/Logger.h>
#include <Common/EventNotifier.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/internal/KeeperCommon.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/logger_useful.h>

#include "AsyncTrxTracker.h"

namespace DB::ErrorCodes
{
extern const int FDB_EXCEPTION;
}

namespace DB::FoundationDB
{

AsyncTrxTracker::~AsyncTrxTracker()
{
    gracefullyCancelAll();
}

void AsyncTrxTracker::gracefullyCancelAll(bool wait)
{
    {
        std::lock_guard guard(mutex);

        if (!cancel_requested)
        {
            auto * log = &Poco::Logger::get("AsyncTrxTracker");
            LOG_TRACE(log, "Try to cancel {} trxs", tokens.size());

            cancel_requested = true;
            if (tokens.empty())
                return;

            for (auto * token : tokens)
                token->cancelFuture();

            /// Notify all subscribers (ReplicatedMergeTree tables) about expired session
            EventNotifier::instance().notify(Coordination::Error::ZSESSIONEXPIRED);
        }
    }

    if (wait)
        waitAllTokenFinished();
}

std::unique_ptr<AsyncTrxTracker::Token> AsyncTrxTracker::newToken()
{
    return std::make_unique<Token>(*this);
}

void AsyncTrxTracker::waitAllTokenFinished()
{
    std::unique_lock lk(mutex);
    cv.wait(lk, [&] { return cancel_requested && tokens.empty(); });
}

AsyncTrxTracker::Token::Token(AsyncTrxTracker & tracker_) : tracker(tracker_)
{
    std::lock_guard guard(tracker.mutex);
    if (tracker.cancel_requested)
        // FIXME: Use a meaningful exception. Currently it is only canceled when the session expires.
        throw KeeperException(Coordination::Error::ZSESSIONEXPIRED);
    tracker.tokens.emplace(this);
}

AsyncTrxTracker::Token::~Token()
{
    std::lock_guard guard(tracker.mutex);
    tracker.tokens.erase(this);
    if (tracker.cancel_requested && tracker.tokens.empty())
        tracker.cv.notify_all();
}

void AsyncTrxCancelToken::assertNotCanceled() const
{
    if (canceled())
        // FIXME: Use a meaningful exception. Currently it is only canceled when the session expires.
        throw KeeperException(Coordination::Error::ZSESSIONEXPIRED);
}

std::unique_ptr<AsyncTrxCancelToken> AsyncTrxCancelToken::splitToken()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "splitToke is not support");
}

void AsyncTrxCancelToken::cancelFuture()
{
    auto * f = future.exchange(nullptr);
    if (f)
        fdb_future_cancel(f);
}

AsyncTrxCancelSource::~AsyncTrxCancelSource()
{
    cancel();
}

std::unique_ptr<AsyncTrxCancelSource::Token> AsyncTrxCancelSource::getToken()
{
    return std::make_unique<Token>(*this);
}

void AsyncTrxCancelSource::cancel()
{
    cancel_requested.store(true);

    std::unique_lock lk(token_mutex);
    if (token)
        token->cancelFuture();
    token_cv.wait(lk, [&]() { return !token; });
}

AsyncTrxCancelSource::Token::Token(AsyncTrxCancelSource & tracker_) : tracker(tracker_)
{
    std::lock_guard guard(tracker.token_mutex);
    if (tracker.token)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Get token from AsyncTrxCancelSource twice");

    tracker.token = this;
}

AsyncTrxCancelSource::Token::~Token()
{
    std::lock_guard guard(tracker.token_mutex);
    tracker.token = nullptr;
    tracker.token_cv.notify_all();
}
}
