#include <memory>

#include <Core/Streaming/CursorTree.h>

#include <Processors/Transforms/WrapShardCursorTransform.h>
#include "Common/logger_useful.h"

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
    if (!chunk.hasChunkInfo(CursorInfo::info_slot))
        return;

    const auto & info = chunk.getChunkInfo(CursorInfo::info_slot);
    const auto * cursor_info = typeid_cast<const CursorInfo *>(info.get());
    chassert(cursor_info);

    CursorDataMap updated;

    for (auto & [storage, cursor] : cursor_info->cursors)
    {
        LOG_DEBUG(&Poco::Logger::get("WrapShardCursorTransform"), "before wrap | storage: {}, cursor: {}", storage, cursorTreeToString(cursor.tree));

        const String & actual_storage = getActualStorage(storage);
        auto & data = (updated[actual_storage] = std::move(cursor));

        if (auto it = changes.keeper_restore_map.find(actual_storage); it != changes.keeper_restore_map.end())
            data.keeper_key = it->second;

        auto wrapped_tree = std::make_shared<CursorTreeNode>();
        wrapped_tree->setSubtree(shard_key, std::move(data.tree));
        data.tree = std::move(wrapped_tree);

        LOG_DEBUG(&Poco::Logger::get("WrapShardCursorTransform"), "after wrap | storage: {}, cursor: {}", actual_storage, cursorTreeToString(data.tree));
    }

    cursor_info->cursors = std::move(updated);
}

const String & WrapShardCursorTransform::getActualStorage(const String & from_info) const
{
    auto it = changes.storage_restore_map.find(from_info);

    if (it == changes.storage_restore_map.end())
        return from_info;

    return it->second;
}

}
