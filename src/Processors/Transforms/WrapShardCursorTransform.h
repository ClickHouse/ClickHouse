#pragma once

#include <Processors/ISimpleTransform.h>
#include <Processors/CursorInfo.h>

#include <Storages/buildQueryTreeForShard.h>

namespace DB
{

class WrapShardCursorTransform : public ISimpleTransform
{
public:
    WrapShardCursorTransform(Block header_, size_t shard_num_, ShardCursorChanges changes_);

    String getName() const override { return "WrapShardCursorTransform"; }

protected:
    void transform(Chunk & chunk) override;

    const String & getActualStorage(const String & real_storage_id) const;
    std::optional<String> getKeeperKey(const String & real_storage_id) const;

private:
    String shard_key;
    ShardCursorChanges changes;
};

}
