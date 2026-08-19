#pragma once

#include <Coordination/Storage/Common.h>

namespace Coordination::Storage
{

/// Locations of the latest versions of all committed nodes: path hash -> which file/memtable has
/// the node's latest update, with a cached pointer to the serialized node.
///
/// Currently this map is not evictable (despite the name) - it must contain an entry for every
/// existing node; only the block pointers inside entries can expire.
/// In future we may add eviction; then we'd have to either keep Memtable nodes unevictable or add
/// another NodeHashMap in Memtable, as currently Memtable has no node lookup capability.
struct NodeRefCache
{
    struct Entry
    {
        /// Cached pointer to the node in memtable or block cache. Its built-in spinlock also
        /// protects `node_offset`: StorageState::getCommittedNode reassigns the pair when reloading
        /// an evicted block, under just a shared storage_mutex lock.
        /// If the node is in memtable, this weak ptr is always alive (block held by Memtable), so
        /// we never have to do node lookup in Memtable.
        mutable BlockWeakPtrWithSpinlock block;
        mutable uint32_t node_offset = 0; // within `block`, if it's still valid

        /// Locates the file/memtable with this node's last update. Survives flushes and merges.
        /// (Not protected by `block`'s spinlock.)
        uint32_t file_seqno = 0;
    };

    /// Doesn't contain removed nodes.
    NodeHashMap<Entry> map;

    bool tryGet(NodePathHash path_hash, NodeRef & out_node, const Entry ** out_entry = nullptr) const;
};

}
