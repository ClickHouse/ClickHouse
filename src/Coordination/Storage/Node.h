#pragma once

#include <Coordination/Storage/Common.h>
#include <Coordination/KeeperCommon.h>
#include <Common/Exception.h>
#include <Common/GroupVarint.h>
#include <IO/VarInt.h>
#include <base/defines.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace Coordination::Storage
{

constexpr uint32_t SERIALIZATION_VERSION_LATEST = 1;

/// Deserialized node. Normally this is short-lived; longer-lived nodes are stored serialized in
/// blocks to save memory, deserialized quickly on demand (see NodeRef).
struct FullNode
{
    DB::KeeperNodeStats stats;

    const char * data_ptr = nullptr;

    NodePath path;
    NodePathHash path_hash = 0; // 0 if not calculated

    uint64_t digest = 0; // 0 if not calculated

    NodeAction action = NodeAction::Remove;

    uint32_t serialized_size = 0; // assigned only when deserializing node

    std::string_view getData() const { return std::string_view(data_ptr, stats.data_size); }
    void setData(std::string_view data) { data_ptr = data.data(); stats.data_size = static_cast<uint32_t>(data.size()); }

    /// Assigns `path_hash` if it's 0.
    NodePathHash getOrCalculatePathHash();
    NodePathWithHash getPathWithHash();

    /// Assigns `digest` if it's 0.
    uint64_t getOrCalculateDigest();
};

/// Memory buffer containing a sequence of serialized znodes.
/// Serialization format is described in BlockData::appendNodeNoResize.
/// The same format is used for znode storage in memory and in files.
///
/// This struct is co-located with its data buffer in one allocation.
/// The BlockData+buffer combination is always wrapped in a (custom-refcounted) BlockPtr.
///
/// BlockData supports appending znodes, as long as there's enough space in the buffer.
/// If the buffer needs to grow, a new BlockData is allocated.
/// After a znode is serialized into a BlockData, the corresponding memory subrange is immutable, so
/// any NodeRef can always safely deserialize the znode without any synchronization, even if it
/// points to a BlockData that's still being written to by another thread.
///
/// A serialized znode may represent either a full znode info (NodeAction::Create or Update) or
/// a tombstone (NodeAction::Remove) saying that the znode was removed (by the file or memtable that
/// contains this block).
///
/// Generally the sequence of znodes in blocks is the source of truth, and everything else is index
/// on top of it. E.g. children sets in Memtable, node hash map in StorageState, bloom filters (TODO),
/// file children index (TODO) - these can all be reconstructed from blocks.
struct BlockData
{
    /// The data buffer starts right after the BlockData, i.e. at `this + 1`.
    uint32_t capacity = 0;
    uint32_t size = 0;

    uint32_t serialization_version = 0;
    uint32_t entries_start = 0; // offset in data() where serialized znodes start

    /// Information for group compression of znode fields.
    uint32_t base_depth = 0;
    uint32_t base_path_len = 0;
    uint32_t base_path_offset = 0; // relative to data()
    uint64_t base_zxid = 0;
    uint64_t base_time = 0;

    bool compatible_digest = false;

    static BlockPtr create(size_t capacity_);

    static void reserve(BlockPtr & block, size_t required_capacity)
    {
        if (block->capacity >= required_capacity)
            return;
        BlockPtr new_block = create(std::max(required_capacity, static_cast<size_t>(block->capacity) * 2));
        uint32_t cap = new_block->capacity;
        memcpy(new_block->data(), block->data(), block->size);
        *new_block = *block; // copy all the fields
        new_block->capacity = cap;
        block = std::move(new_block);
    }

    /// The data buffer is in memory immediately after the BlockData struct.
    char * data() { return reinterpret_cast<char *>(this + 1); }
    const char * data() const { return reinterpret_cast<const char *>(this + 1); }

    /// Estimate how many bytes serialized node would take. Never underestimates.
    size_t nodeSerializedSizeUpperBound(const FullNode & node) const;

    /// Initializes the block, taking base values for delta encoding from `node`.
    /// `node` itself is not written.
    static void writeHeader(BlockPtr & block, const FullNode & node);

    /// May reassign `block` if we hit capacity and had to allocate a bigger block.
    /// Automatically calls writeHeader if block is uninitialized.
    static NodeRef appendNode(BlockPtr & block, FullNode & node);

    /// The caller must ensure that there's enough space (nodeSerializedSizeUpperBound) and that
    /// writeHeader was called.
    static NodeRef appendNodeNoResize(BlockPtr block, FullNode & node);

    /// If the node fits in this block's capacity, append it here, and return false.
    /// Otherwise create a new block with capacity at least new_block_capacity, append the node to
    /// it, and return true. `block` may be nullptr, then we always start a new block.
    static bool appendNodeOrStartNewBlock(const BlockPtr & block, FullNode & node, size_t new_block_capacity, BlockPtr & out_new_block, NodeRef & out_node_ref);

    /// Deserializes the block header and assigns fields. Call after writing the block data
    /// (that presumably comes from file) to `data()` and assigning `size` and
    /// `serialization_version` (which is not stored per block, only in file header).
    void readHeader();

    /// Make a copy with capacity = size.
    BlockPtr copyAndShrinkToFit() const;

private:
    /// Constructed only via create()/reserve(), which allocate the object together with its trailing
    /// data buffer. Keep the default constructor private to prevent accidental construction that
    /// would bypass that (and leave data() pointing at unowned memory).
    BlockData() = default;
};

/// Decode/encode a field stored as a zigzag delta relative to a base value. The arithmetic is done
/// on unsigned types so wraparound is well-defined (signed overflow would be UB).
/// (Note: signed and unsigned addition are the same operation. Ditto for subtraction.)
inline uint64_t decodeZigZagDelta64(uint64_t base, uint64_t zigzag_delta)
{
    return base + static_cast<uint64_t>(DB::decodeZigZag(zigzag_delta));
}
inline uint64_t encodeZigZagDelta64(uint64_t base, uint64_t value)
{
    return DB::encodeZigZag(static_cast<int64_t>(value - base));
}
inline uint32_t decodeZigZagDelta32(uint32_t base, uint32_t zigzag_delta)
{
    return base + static_cast<uint32_t>(DB::decodeZigZag32(zigzag_delta));
}
inline uint32_t encodeZigZagDelta32(uint32_t base, uint32_t value)
{
    return DB::encodeZigZag32(static_cast<int32_t>(value - base));
}

}
