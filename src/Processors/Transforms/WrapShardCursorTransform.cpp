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

    for (auto & [stream_name, cursor] : cursor_info->cursors)
    {
        const String & actual_stream_name = getActualStreamName(stream_name);
        auto & data = (updated[actual_stream_name] = std::move(cursor));

        data.keeper_key = getKeeperKey(stream_name);

        auto wrapped_tree = std::make_shared<CursorTreeNode>();
        wrapped_tree->setSubtree(shard_key, std::move(data.tree));
        data.tree = std::move(wrapped_tree);
    }

    cursor_info->cursors = std::move(updated);
}

const String & WrapShardCursorTransform::getActualStreamName(const String & stream_name) const
{
    if (auto it = changes.stream_name_restore_map.find(stream_name); it != changes.stream_name_restore_map.end())
        return it->second;

    return stream_name;
}

std::optional<String> WrapShardCursorTransform::getKeeperKey(const String & stream_name) const
{
    if (auto it = changes.keeper_restore_map.find(stream_name); it != changes.keeper_restore_map.end())
        return it->second;

    return std::nullopt;
}

}
