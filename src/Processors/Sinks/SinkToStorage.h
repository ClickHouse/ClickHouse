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

    virtual std::shared_ptr<DedupTokenInfo> setDeduplicationTokenForChildren(Chunk & chunk) const
    {
        auto token_info = chunk.getChunkInfos().get<DedupTokenInfo>();
        if (token_info)
            return token_info;

        auto block_dedup_token_for_children = std::make_shared<DedupTokenInfo>("");
        chunk.getChunkInfos().add(block_dedup_token_for_children);
        return block_dedup_token_for_children;
    }

    virtual std::shared_ptr<DedupTokenInfo> getDeduplicationTokenForChildren(Chunk & chunk) const
    {
        return chunk.getChunkInfos().get<DedupTokenInfo>();
    }

    virtual void fillDeduplicationTokenForChildren(Chunk & chunk) const
    {
        SipHash hash;
        for (const auto & colunm: chunk.getColumns())
        {
            colunm->updateHashFast(hash);
        }
        const auto hash_value = hash.get128();

        chunk.getChunkInfos().get<DedupTokenInfo>()->addTokenPart(
            fmt::format(":hash-{}", toString(hash_value.items[0]) + "_" + toString(hash_value.items[1])));
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
