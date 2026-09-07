#include <Coordination/Storage/NodeRefCache.h>

namespace Coordination::Storage
{

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
