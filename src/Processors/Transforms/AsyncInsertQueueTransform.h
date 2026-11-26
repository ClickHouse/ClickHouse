#pragma once

#include <Common/DequeWithMemoryTracking.h>

#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/TableLockHolder.h>
#include <Common/Logger.h>

#include <optional>

namespace DB
{

class AsynchronousInsertQueue;
class PushingAsyncPipelineExecutor;

/// The block `AsyncInsertQueueTransform` diverts, with the query to push it with.
struct AsyncInsertDivertedBlock
{
    ASTPtr query;
    std::optional<Block> block;
};

/// Runs the pipeline of a local `INSERT ... SELECT` that may divert its result, destroys it, and only then
/// pushes the diverted block to the async insert queue and waits for the flush. The flush locks its tables
/// under a query id of its own, so a lock the query still held could deadlock with a writer queued between
/// the two. Once the pipeline is gone, the query holds no lock.
class AsyncInsertSelectSource final : public ISource
{
public:
    AsyncInsertSelectSource(
        QueryPipeline pipeline_,
        std::shared_ptr<AsyncInsertDivertedBlock> diverted_,
        AsynchronousInsertQueue * queue_,
        ContextPtr context_,
        UInt64 wait_timeout_ms_,
        bool wait_for_flush_);

    String getName() const override { return "AsyncInsertSelectSource"; }

protected:
    Chunk generate() override;

private:
    QueryPipeline pipeline;
    std::shared_ptr<AsyncInsertDivertedBlock> diverted;
    AsynchronousInsertQueue * queue;
    ContextPtr context;
    UInt64 wait_timeout_ms;
    bool wait_for_flush;
};

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
        ContextMutablePtr context_,
        ASTPtr query_ast_,
        Names insert_column_names_,
        UInt64 max_data_size_,
        TableLockHolder destination_lock_,
        std::shared_ptr<AsyncInsertDivertedBlock> diverted_,
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
    /// `disqualify` without a chunk of its own to push.
    void fallBack();

    ContextMutablePtr context;
    ASTPtr query_ast;
    /// Frozen onto the AST clone pushed to the queue instead of re-resolved against a possibly newer schema.
    Names insert_column_names;
    UInt64 max_data_size;
    /// The destination's share lock. Owned by this pipeline rather than the outer one, so it goes with this
    /// pipeline before `AsyncInsertSelectSource` pushes the diverted block.
    TableLockHolder destination_lock;
    std::shared_ptr<AsyncInsertDivertedBlock> diverted;
    LoggerPtr logger;

    /// Frozen at query start (see `InterpreterInsertQuery::addInsertToSelectPipeline`), so the
    /// fallback pipeline, built lazily here, sees the same schema/dedup/view decisions as an eagerly
    /// built one would, not ones re-derived from possibly newer catalog state.
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
    DequeWithMemoryTracking<Chunk> pending;
    /// Points into `pending`, valid until eligibility is lost or the block is diverted; `pending`'s
    /// push_back does not invalidate it.
    Chunk * held = nullptr;
    /// Cleared by a second non-empty block or an oversized first one; once false, every chunk goes
    /// straight to the fallback pipeline instead.
    bool queued_eligible = true;
};

}
