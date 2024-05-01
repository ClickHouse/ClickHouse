#include <Core/Streaming/CursorZkUtils.h>

#include <Processors/CursorInfo.h>
#include <Processors/Transforms/UpdateKeeperCursorsTransform.h>

namespace DB
{

UpdateKeeperCursorsTransform::UpdateKeeperCursorsTransform(Block header_, zkutil::ZooKeeperPtr zk_)
    : ISimpleTransform(header_, header_, false), zk(std::move(zk_))
{
}

void UpdateKeeperCursorsTransform::transform(Chunk & chunk)
{
    if (!chunk.hasChunkInfo(CursorInfo::info_slot))
        return;

    const auto & info = chunk.getChunkInfo(CursorInfo::info_slot);
    const auto * cursor_info = typeid_cast<const CursorInfo *>(info.get());
    chassert(cursor_info);

    Coordination::Requests ops;
    getUpdateCursorOps(zk, ops, cursor_info->cursors);

    zk->multi(ops);
}

}
