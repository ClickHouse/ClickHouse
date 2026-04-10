#pragma once

#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IASTHash.h>
#include <base/UUID.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB
{

/// A queue that collects identical concurrent SELECT queries and batches them together.
///
/// When multiple queries with the same normalized AST arrive within a short time window,
/// only the first ("leader") query is executed. The leader writes its result into the
/// query result cache. Subsequent ("follower") queries in the batch wait for the leader
/// to finish and then read from the cache.
///
/// Batching is per-user: queries from different users are never mixed into the same batch.
///
/// The design is inspired by AsynchronousInsertQueue but for SELECT queries.
class QueryBatchingQueue : public WithContext
{
public:
    using Milliseconds = std::chrono::milliseconds;

    explicit QueryBatchingQueue(ContextPtr context_);
    ~QueryBatchingQueue();

    /// Returned by tryBatchQuery to indicate the query's role in a batch.
    struct BatchResult
    {
        enum class Role : uint8_t
        {
            /// This query is the leader — it should execute normally with cache write enabled.
            Leader,
            /// This query is a follower — it should read from cache after the leader finishes.
            Follower,
        };

        Role role;

        /// The hash identifying this batch. The leader must pass this back to notifyBatchComplete.
        UInt128 batch_key_hash;

        /// For followers: a future that becomes ready when the leader has finished.
        /// For leaders: empty.
        std::shared_future<void> leader_done;
    };

    /// Try to batch this query. Returns a BatchResult indicating the query's role.
    /// If batching is not applicable (e.g., non-SELECT), returns std::nullopt.
    std::optional<BatchResult> tryBatchQuery(
        const ASTPtr & ast,
        const ContextPtr & query_context);

    /// Called by the leader after it has finished executing (successfully or not).
    void notifyBatchComplete(UInt128 batch_key_hash);

    void shutdown();

private:
    /// A batch key identifies a group of queries that can be batched together.
    struct BatchKey
    {
        IASTHash ast_hash;
        std::optional<UUID> user_id;

        bool operator==(const BatchKey & other) const;
    };

    /// An active batch collecting queries.
    struct Batch
    {
        BatchKey key;
        UInt128 key_hash;
        size_t query_count = 0;
        size_t max_batch_size = 100;
        std::chrono::steady_clock::time_point created_at;
        std::promise<void> leader_done_promise;
        std::shared_future<void> leader_done_future;

        Batch()
            : leader_done_future(leader_done_promise.get_future().share())
        {
        }
    };

    using BatchPtr = std::shared_ptr<Batch>;
    using BatchMap = std::unordered_map<UInt128, BatchPtr>;

    mutable std::mutex mutex;
    std::condition_variable cv;
    BatchMap active_batches;

    std::atomic<bool> is_shutdown{false};
    ThreadFromGlobalPool flush_thread;

    LoggerPtr log = getLogger("QueryBatchingQueue");

    /// Background thread that flushes expired batches.
    void processExpiredBatches();

    /// Compute a combined hash for the batch key.
    static UInt128 computeBatchKeyHash(const BatchKey & key);
};

using QueryBatchingQueuePtr = std::shared_ptr<QueryBatchingQueue>;

}
