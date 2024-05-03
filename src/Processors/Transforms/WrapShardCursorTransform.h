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

    const String & getActualStreamName(const String & stream_name) const;
    std::optional<String> getKeeperKey(const String & stream_name) const;

private:
    String shard_key;
    ShardCursorChanges changes;
};

}
