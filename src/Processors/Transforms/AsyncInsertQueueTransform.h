#pragma once

#include <deque>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Storages/TableLockHolder.h>
#include <Common/Logger.h>

namespace DB
{

class AsynchronousInsertQueue;
class QueryPipeline;
class PushingAsyncPipelineExecutor;

/// Additive one-block lookahead for a local `INSERT ... SELECT` under `async_insert`. A result
/// that fits in a single block within `async_insert_max_data_size` is diverted to the async queue;
/// any other shape is written through a synchronous fallback pipeline, built lazily (see
/// `startFallback`) so a destination sink's start-time side effect (e.g. `AliasSink` opening its
/// nested `INSERT`, or `MergeTreeSink` delaying on "too many parts") is only paid once the fallback
/// is actually needed, not for every query that merely takes this route.
class AsyncInsertQueueTransform final : public ExceptionKeepingTransform
{
public:
    AsyncInsertQueueTransform(
        SharedHeader header_,
        AsynchronousInsertQueue * queue_,
        ContextMutablePtr context_,
        ASTPtr query_ast_,
        Names insert_column_names_,
        UInt64 max_data_size_,
        UInt64 wait_timeout_ms_,
        bool wait_for_flush_,
        TableLockHolder destination_lock_,
        InsertDependenciesBuilder::ConstPtr insert_dependencies_,
        StoragePtr table_,
        size_t max_threads_,
        bool no_squash_,
        bool async_insert_flag_);

    ~AsyncInsertQueueTransform() override;

    String getName() const override { return "AsyncInsertQueueTransform"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    bool canGenerate() override;
    GenerateResult getRemaining() override;

private:
    /// Builds and starts the fallback pipeline on first use, then replays whatever was buffered in
    /// `pending` while eligibility was still open, in arrival order.
    void startFallback();
    /// Ends eligibility for the queue route: starts the fallback if it is not running yet, replays
    /// `pending` (which still holds `*held`, if any, at its original position), then pushes `chunk`.
    void disqualify(Chunk chunk);

    AsynchronousInsertQueue * queue;
    ContextMutablePtr context;
    ASTPtr query_ast;
    /// Frozen onto the AST clone pushed to the queue instead of re-resolved against a possibly newer schema.
    Names insert_column_names;
    UInt64 max_data_size;
    UInt64 wait_timeout_ms;
    bool wait_for_flush;
    /// The destination's share lock, owned here so it can be dropped once the queue has the block:
    /// the flush re-acquires it under its own query id, which cannot join this lock's reader group
    /// once an exclusive locker is queued. Kept for the whole pipeline when the block is not diverted.
    /// `insert_dependencies` below holds share locks of its own and goes at the same point.
    TableLockHolder destination_lock;
    LoggerPtr logger;

    /// Frozen at query start (see `InterpreterInsertQuery::addInsertToSelectPipeline`), so the
    /// fallback pipeline, built lazily here, sees the same schema/dedup/view decisions as an eagerly
    /// built one would, not ones re-derived from possibly newer catalog state. Dropped once a block is
    /// diverted: it keeps a share lock per node of the dependency path, the destination included, and
    /// nothing needs it after the divert.
    InsertDependenciesBuilder::ConstPtr insert_dependencies;
    StoragePtr table;
    size_t max_threads;
    bool no_squash;
    bool async_insert_flag;

    std::unique_ptr<QueryPipeline> fallback_pipeline;
    /// Async, not `PushingPipelineExecutor`: that one is documented as "always executed in single
    /// thread", which would run the whole destination side (squashing and every parallel sink chain
    /// `buildPushPipelineFromDependencies` built) on the one thread pushing into it, discarding the
    /// `max_insert_threads` parallelism a plain `INSERT ... SELECT` gets.
    std::unique_ptr<PushingAsyncPipelineExecutor> fallback_executor;

    /// All chunks buffered, in arrival order, while eligibility is still open: zero-row chunks (kept
    /// for the chunk info they may carry, e.g. from `RestoreChunkInfosTransform`) and, once seen, the
    /// single held candidate itself (`held` points at its slot). Replayed into the fallback pipeline
    /// if eligibility is lost, discarded otherwise (the queue divert only ever needs `*held`).
    std::deque<Chunk> pending;
    /// Points into `pending`, valid until eligibility is lost or the block is diverted; `pending`'s
    /// push_back does not invalidate it.
    Chunk * held = nullptr;
    /// Cleared by a second non-empty block or an oversized first one; once false, every chunk goes
    /// straight to the fallback pipeline instead.
    bool queued_eligible = true;
};

}
