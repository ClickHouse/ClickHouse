#include <Coordination/Storage/NodeRefCache.h>

namespace Coordination::Storage
{

NodeRefCache::Entry * NodeRefCache::findEntry(NodePathHash path_hash)
{
    auto * cell = map.find(path_hash);
    return cell ? &cell->getMapped() : nullptr;
}

const NodeRefCache::Entry * NodeRefCache::findEntry(NodePathHash path_hash) const
{
    const auto * cell = map.find(path_hash);
    return cell ? &cell->getMapped() : nullptr;
}

NodeRefCache::Entry & NodeRefCache::getOrInsertEntry(NodePathHash path_hash)
{
    return map[path_hash];
}

void NodeRefCache::eraseEntry(NodePathHash path_hash, Entry & entry)
{
    /// `HashTable::erase` clears the cell without running the value's destructor, so we have to
    /// do its cleanup manually. This is why `map` is private.
    entry.block.store(nullptr);
    bool erased = map.erase(path_hash);
    chassert(erased);
}

bool NodeRefCache::tryGet(NodePathHash path_hash, NodeRef & out_node, const Entry ** out_entry) const
{
    const auto * lookup = map.find(path_hash);
    if (!lookup)
        return false;
    const NodeRefCache::Entry & info = lookup->getMapped();
    if (out_entry)
        *out_entry = &info;

    {
        std::lock_guard guard(info.block);

        if (BlockPtr block = info.block.get())
        {
            out_node = NodeRef{.action = NodeAction::Create, .offset = info.node_offset, .block = std::move(block)};
            return true;
        }
    }

    return false;
}

}
