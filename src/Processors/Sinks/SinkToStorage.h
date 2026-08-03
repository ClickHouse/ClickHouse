#pragma once
#include <Storages/TableLockHolder.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

#include <functional>
#include <mutex>

namespace DB
{

class Context;

/// Shared by all the sinks that write into the same table on behalf of a single INSERT query.
/// With `max_insert_threads` greater than one the query writes through several sinks running in
/// parallel, and their `onStart` calls are not ordered with respect to each other's writes.
using InsertStartGatePtr = std::shared_ptr<std::once_flag>;

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

    void setInsertStartGate(InsertStartGatePtr gate) { insert_start_gate = std::move(gate); }

protected:
    virtual void consume(Chunk & chunk) = 0;

    /// Runs `check` once for the whole INSERT query, before any of its parallel sinks writes anything:
    /// the first sink to get here runs it, the others wait until it is finished. This is required for the
    /// checks that are only allowed to throw before the query has written anything - the `Too many parts`
    /// check - because a sink that starts late would otherwise count the parts the query itself has just
    /// written and reject the query in the middle of it.
    void runOnceBeforeFirstWrite(const std::function<void()> & check);

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
