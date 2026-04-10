#include <Interpreters/QueryBatchingQueue.h>

#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool use_query_batching;
    extern const SettingsMilliseconds query_batching_max_wait_ms;
    extern const SettingsUInt64 query_batching_max_batch_size;
}

QueryBatchingQueue::QueryBatchingQueue(ContextPtr context_)
    : WithContext(context_)
{
    flush_thread = ThreadFromGlobalPool([this] { processExpiredBatches(); });
}

QueryBatchingQueue::~QueryBatchingQueue()
{
    shutdown();
}

void QueryBatchingQueue::shutdown()
{
    bool expected = false;
    if (!is_shutdown.compare_exchange_strong(expected, true))
        return;

    {
        std::lock_guard lock(mutex);
        /// Notify all waiting batches that we are shutting down.
        for (auto & [_, batch] : active_batches)
        {
            try
            {
                batch->leader_done_promise.set_value();
            }
            catch (const std::future_error &)
            {
                /// Promise already satisfied — that's fine.
            }
        }
        active_batches.clear();
    }

    cv.notify_all();

    if (flush_thread.joinable())
        flush_thread.join();

    LOG_DEBUG(log, "QueryBatchingQueue shut down");
}

bool QueryBatchingQueue::BatchKey::operator==(const BatchKey & other) const
{
    return ast_hash == other.ast_hash && user_id == other.user_id;
}

UInt128 QueryBatchingQueue::computeBatchKeyHash(const BatchKey & key)
{
    SipHash hasher;
    hasher.update(key.ast_hash.low64);
    hasher.update(key.ast_hash.high64);
    if (key.user_id)
        hasher.update(key.user_id->toUnderType());
    return hasher.get128();
}

std::optional<QueryBatchingQueue::BatchResult> QueryBatchingQueue::tryBatchQuery(
    const ASTPtr & ast,
    const ContextPtr & query_context)
{
    if (is_shutdown)
        return std::nullopt;

    /// Only batch SELECT queries.
    if (!ast->as<ASTSelectQuery>() && !ast->as<ASTSelectWithUnionQuery>())
        return std::nullopt;

    const auto & settings = query_context->getSettingsRef();
    if (!settings[Setting::use_query_batching])
        return std::nullopt;

    /// Compute the batch key from the AST hash and user ID.
    BatchKey key;
    key.ast_hash = ast->getTreeHash(/*ignore_aliases=*/false);
    key.user_id = query_context->getUserID();

    UInt128 key_hash = computeBatchKeyHash(key);

    std::lock_guard lock(mutex);

    auto it = active_batches.find(key_hash);
    if (it != active_batches.end())
    {
        /// A batch already exists for this key — this query is a follower.
        auto & batch = it->second;

        /// If the batch has reached its maximum size, don't join it — execute independently.
        if (batch->query_count >= batch->max_batch_size)
        {
            LOG_TRACE(log, "Batch full (key_hash={}, count={}), query will execute independently",
                key_hash, batch->query_count);
            return std::nullopt;
        }

        batch->query_count++;

        LOG_TRACE(log, "Query joined existing batch (key_hash={}, count={})", key_hash, batch->query_count);

        return BatchResult{
            .role = BatchResult::Role::Follower,
            .batch_key_hash = key_hash,
            .leader_done = batch->leader_done_future,
        };
    }

    /// No existing batch — this query becomes the leader.
    auto batch = std::make_shared<Batch>();
    batch->key = key;
    batch->key_hash = key_hash;
    batch->query_count = 1;
    batch->max_batch_size = settings[Setting::query_batching_max_batch_size];
    batch->created_at = std::chrono::steady_clock::now();

    active_batches[key_hash] = batch;

    LOG_TRACE(log, "Query started new batch as leader (key_hash={})", key_hash);

    cv.notify_one();

    return BatchResult{
        .role = BatchResult::Role::Leader,
        .batch_key_hash = key_hash,
        .leader_done = {},
    };
}

void QueryBatchingQueue::notifyBatchComplete(UInt128 batch_key_hash)
{
    std::lock_guard lock(mutex);

    auto it = active_batches.find(batch_key_hash);
    if (it == active_batches.end())
        return;

    auto & batch = it->second;

    LOG_TRACE(log, "Batch complete (key_hash={}, served {} queries)", batch_key_hash, batch->query_count);

    try
    {
        batch->leader_done_promise.set_value();
    }
    catch (const std::future_error &)
    {
        /// Promise already satisfied.
    }

    active_batches.erase(it);
}

void QueryBatchingQueue::processExpiredBatches()
{
    setThreadName("QryBatchFlush");

    while (!is_shutdown)
    {
        std::vector<BatchPtr> expired_batches;

        {
            std::unique_lock lock(mutex);

            /// Wait for either new batches or a short polling interval.
            cv.wait_for(lock, std::chrono::milliseconds(50), [this]
            {
                return is_shutdown.load() || !active_batches.empty();
            });

            if (is_shutdown)
                return;

            auto now = std::chrono::steady_clock::now();

            /// Get the max wait from the global context settings.
            auto max_wait_ms = Milliseconds(getContext()->getSettingsRef()[Setting::query_batching_max_wait_ms].totalMilliseconds());
            if (max_wait_ms == Milliseconds::zero())
                max_wait_ms = Milliseconds(100); /// Fallback default.

            for (auto it = active_batches.begin(); it != active_batches.end();)
            {
                auto & batch = it->second;
                auto elapsed = std::chrono::duration_cast<Milliseconds>(now - batch->created_at);

                if (elapsed >= max_wait_ms)
                {
                    LOG_TRACE(log, "Batch expired (key_hash={}, age={}ms, count={})",
                        batch->key_hash, elapsed.count(), batch->query_count);

                    expired_batches.push_back(batch);
                    it = active_batches.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }

        /// Notify expired batches outside the lock.
        for (auto & batch : expired_batches)
        {
            try
            {
                batch->leader_done_promise.set_value();
            }
            catch (const std::future_error &)
            {
                /// Already satisfied.
            }
        }
    }
}

}
