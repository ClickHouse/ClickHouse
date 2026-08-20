#pragma once

#include <deque>
#include <optional>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Storages/TableLockHolder.h>
#include <Common/Logger.h>

namespace DB
{

class AsynchronousInsertQueue;

/// Additive one-block lookahead for a local `INSERT ... SELECT` under `async_insert`. A result
/// that fits in a single block within `async_insert_max_data_size` is diverted to the async queue;
/// any other shape passes through unchanged to the normal sink chain.
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
        TableLockHolder destination_lock_);

    String getName() const override { return "AsyncInsertQueueTransform"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    bool canGenerate() override;
    GenerateResult getRemaining() override;

private:
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
    TableLockHolder destination_lock;
    LoggerPtr logger;

    /// The single block held while eligibility is still open, materialized and sized.
    std::optional<Chunk> held;
    /// Cleared by a second non-empty block or an oversized first one; once false, every chunk
    /// only passes through.
    bool queued_eligible = true;
    std::deque<Chunk> pending;
};

}
