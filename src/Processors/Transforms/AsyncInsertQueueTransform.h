#pragma once

#include <deque>
#include <optional>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

namespace DB
{

class AsynchronousInsertQueue;

/// Additive one-block lookahead for a local `INSERT ... SELECT` under `async_insert`, spliced
/// into the sink chain that `InterpreterInsertQuery::addInsertToSelectPipeline` already builds.
/// A result that fits in a single block within `async_insert_max_data_size` is diverted to the
/// async queue instead of the sink chain below; any other shape (a second block, an oversized
/// first block, zero rows) passes through unchanged, so the rest of the query runs exactly as the
/// synchronous pipeline would without this transform.
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
        UInt64 wait_timeout_ms_);

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
    /// Frozen onto the AST clone pushed to the queue, so a concurrent `ALTER TABLE ADD COLUMN` or
    /// a column transformer (`* EXCEPT c`) cannot re-resolve against a schema newer than the one
    /// this pipeline was built against.
    Names insert_column_names;
    UInt64 max_data_size;
    UInt64 wait_timeout_ms;

    /// The single block held while eligibility is still open, materialized and sized.
    std::optional<Chunk> held;
    /// Cleared by a second non-empty block or an oversized first one; once false, every chunk
    /// only passes through.
    bool queued_eligible = true;
    std::deque<Chunk> pending;
};

}
