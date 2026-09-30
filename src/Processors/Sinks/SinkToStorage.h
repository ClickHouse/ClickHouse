#pragma once
#include <functional>
#include <optional>
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

    void setCommitOrder(
        std::function<bool(size_t)> allowed,
        std::function<bool()> failed,
        std::function<void()> done,
        std::function<void()> fail,
        std::function<bool(size_t)> epoch_done,
        std::function<int()> schedule_fd,
        std::function<void()> drain);

protected:
    virtual void consume(Chunk & chunk) = 0;
    virtual bool orderedCommitPending() const { return false; }
    virtual void commitDeferredChunk() {}
    virtual void abandonDeferredChunk() {}

    bool readyForCommit() const override;
    bool readyForNextChunk() const override;
    int commitWaitFD() const override;
    void drainCommitWait() override;

    void finishCommitStep();
    void failCommitOrder();
    void holdNextChunk();

    struct CommitOrder
    {
        std::function<bool(size_t)> allowed;
        std::function<bool()> failed;
        std::function<void()> done;
        std::function<void()> fail;
        std::function<bool(size_t)> epoch_done;
        std::function<int()> schedule_fd;
        std::function<void()> drain;
        size_t epoch = 0;
        mutable bool hold_next = false;
    };

    std::optional<CommitOrder> commit_order;

private:
    std::vector<TableLockHolder> table_locks;
    std::vector<std::shared_ptr<const Context>> interpreter_context;

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
