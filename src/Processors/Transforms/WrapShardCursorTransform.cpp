#include <memory>

#include <Core/Streaming/CursorTree.h>

#include <Processors/Transforms/WrapShardCursorTransform.h>

namespace DB
{

WrapShardCursorTransform::WrapShardCursorTransform(
    Block header_, size_t shard_num_, ShardCursorChanges changes_)
    : ISimpleTransform(header_, header_, false)
    , shard_key(fmt::format("{}", shard_num_))
    , changes(std::move(changes_))
{
}

void WrapShardCursorTransform::transform(Chunk & chunk)
{
    if (!chunk.hasChunkInfo(CursorInfo::INFO_SLOT))
        return;

    const auto & info = chunk.getChunkInfo(CursorInfo::INFO_SLOT);
    const auto * cursor_info = typeid_cast<const CursorInfo *>(info.get());
    chassert(cursor_info);

    CursorDataMap updated;

    for (auto & [real_storage_id, cursor] : cursor_info->cursors)
    {
        const String & actual_storage = getActualStorage(real_storage_id);
        auto & data = (updated[actual_storage] = std::move(cursor));

        data.keeper_key = getKeeperKey(real_storage_id);

        auto wrapped_tree = std::make_shared<CursorTreeNode>();
        wrapped_tree->setSubtree(shard_key, std::move(data.tree));
        data.tree = std::move(wrapped_tree);
    }

    cursor_info->cursors = std::move(updated);
}

const String & WrapShardCursorTransform::getActualStorage(const String & real_storage_id) const
{
    if (auto it = changes.storage_restore_map.find(real_storage_id); it != changes.storage_restore_map.end())
        return it->second;

    return  real_storage_id;
}

std::optional<String> WrapShardCursorTransform::getKeeperKey(const String & real_storage_id) const
{
    if (auto it = changes.keeper_restore_map.find(real_storage_id); it != changes.keeper_restore_map.end())
        return it->second;

    return std::nullopt;
}

}
