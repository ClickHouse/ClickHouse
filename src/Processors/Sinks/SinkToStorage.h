#pragma once
#include <memory>
#include <Storages/TableLockHolder.h>
#include <Storages/IStorage.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <fmt/core.h>
#include "Processors/Transforms/NumberBlocksTransform.h"

namespace DB
{

/// Sink which is returned from Storage::write.
class SinkToStorage : public ExceptionKeepingTransform
{
/// PartitionedSink owns nested sinks.
friend class PartitionedSink;

public:
    explicit SinkToStorage(const Block & header);

    const Block & getHeader() const { return inputs.front().getHeader(); }
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

protected:
    virtual void consume(Chunk & chunk) = 0;
    virtual bool lastBlockIsDuplicate() const { return false; }

    void fillDeduplicationTokenForChildren(Chunk & chunk) const
    {
        auto token_info = chunk.getChunkInfos().get<DedupTokenInfo>();
        if (token_info)
            return;

        SipHash hash;
        for (const auto & colunm: chunk.getColumns())
        {
            colunm->updateHashFast(hash);
        }
        const auto hash_value = hash.get128();

        chunk.getChunkInfos().add(std::make_shared<DedupTokenInfo>(
            fmt::format(":hash-{}", toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]))
        ));
    }

private:
    std::vector<TableLockHolder> table_locks;

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

    Chunk cur_chunk;
};

using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;


class NullSinkToStorage : public SinkToStorage
{
public:
    using SinkToStorage::SinkToStorage;
    std::string getName() const override { return "NullSinkToStorage"; }
    void consume(Chunk &) override {}
};

using SinkPtr = std::shared_ptr<SinkToStorage>;
}
