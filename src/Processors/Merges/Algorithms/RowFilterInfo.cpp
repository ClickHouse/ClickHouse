#include <Processors/Merges/Algorithms/RowFilterInfo.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ChunkInfo::Ptr RowFilterInfo::merge(const Ptr &) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunks carrying a RowFilterInfo cannot be combined");
}

const IColumnFilter * getRowFilterMask(const Chunk & chunk)
{
    const auto info = chunk.getChunkInfos().get<RowFilterInfo>();
    if (!info)
        return nullptr;

    chassert(info->mask.size() == chunk.getNumRows(),
        fmt::format("RowFilterInfo holds a mask of {} entries for a chunk of {} rows",
            info->mask.size(), chunk.getNumRows()));

    return &info->mask;
}

}
