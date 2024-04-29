#include <utility>

#include <Core/Streaming/CursorMerger.h>
#include <Core/Streaming/CursorTree.h>

namespace DB
{

void CursorMerger::add(const CursorDataMap & data_map)
{
    for (const auto & [storage, data] : data_map)
    {
        if (auto it = merged_data_map.find(storage); it != merged_data_map.end())
        {
            chassert(it->second.tree);
            chassert(it->second.keeper_key == data.keeper_key);
            mergeCursors(it->second.tree, data.tree);
        }
        else
            merged_data_map[storage] = data;
    }
}

CursorDataMap CursorMerger::finalize()
{
    return std::exchange(merged_data_map, {});
}

}
