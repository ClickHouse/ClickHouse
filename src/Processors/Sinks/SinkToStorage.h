#pragma once
#include <Interpreters/InsertStartGates.h>
#include <Storages/TableLockHolder.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

namespace DB
{

class Context;

/// Sink which is returned from Storage::write.
class SinkToStorage : public ExceptionKeepingTransform
{
/// PartitionedSink owns nested sinks.
friend class PartitionedSink;
friend class DeltaLakePartitionedSink;

public:
    explicit SinkToStorage(SharedHeader header);

    const Block & getHeader() const { return inputs.front().getHeader(); }
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }
    void addInterpreterContext(std::shared_ptr<const Context> context) { interpreter_context.emplace_back(std::move(context)); }

    virtual void setHasDependentMaterializedViews(bool /*has_dependent_views*/) {}

    /// Registers the pre-write check of this sink in the gate (see `getPreWriteCheck`).
    void setInsertStartGate(InsertStartGatePtr gate);

    /// A sink that forwards the write through a nested INSERT running in this query's context
    /// (`AliasSink`) receives the per-query registry its gate came from and threads it into that
    /// nested INSERT, so the sinks created there share the gates of the outer query instead of
    /// creating their own. See InsertStartGates.h.
    virtual void setInsertStartGateRegistry(InsertStartGatesPtr /*gates*/) {}

protected:
    virtual void consume(Chunk & chunk) = 0;

    /// The check that may reject the query only before it has written anything - the `Too many parts`
    /// check. Empty if the sink has none. It must not keep the sink alive, see `InsertStartGate::Check`.
    virtual InsertStartGate::Check getPreWriteCheck() const { return {}; }

    /// Runs the pre-write checks once for the whole INSERT query, before any of its parallel sinks writes
    /// anything: the first sink to get here runs the checks of all the sinks sharing its gate, the others
    /// wait until it is finished. A sink that starts late would otherwise count the parts the query itself
    /// has just written and reject the query in the middle of it.
    void runPreWriteCheck();

private:
    std::vector<TableLockHolder> table_locks;
    std::vector<std::shared_ptr<const Context>> interpreter_context;

    InsertStartGatePtr insert_start_gate;

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

    Chunk cur_chunk;
};

using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;


class NullSinkToStorage final : public SinkToStorage
{
public:
    using SinkToStorage::SinkToStorage;
    std::string getName() const override { return "NullSinkToStorage"; }
    void consume(Chunk &) override {}
};

using SinkPtr = std::shared_ptr<SinkToStorage>;
}
