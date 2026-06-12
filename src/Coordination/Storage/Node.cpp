#include <Coordination/Storage/Node.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <base/unaligned.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

namespace Coordination::Storage
{

using DB::Exception;
namespace ErrorCodes = DB::ErrorCodes;

namespace
{
    /// Block header:
    ///    u32 header_size (includes this field itself)
    ///    u32 base_depth
    ///    u32 base_path_len
    ///    u64 base_zxid
    ///    u64 base_time
    ///    base_path (base_path_len bytes)
    constexpr size_t BLOCK_HEADER_SCALARS_SIZE = sizeof(uint32_t) * 3 + sizeof(uint64_t) * 2;
}

std::optional<NodeAction> combineActions(std::optional<NodeAction> first, NodeAction second, bool strict)
{
    if (!strict || !first.has_value())
        return second;

    if (first == NodeAction::Create && second == NodeAction::Update)
        return NodeAction::Create;
    if (first == NodeAction::Create && second == NodeAction::Remove)
        return std::nullopt;
    if (first == NodeAction::Update && second == NodeAction::Update)
        return NodeAction::Update;
    if (first == NodeAction::Update && second == NodeAction::Remove)
        return NodeAction::Remove;
    if (first == NodeAction::Remove && second == NodeAction::Create)
        return NodeAction::Update;

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Can't combine NodeAction {} followed by {}",
        uint32_t(*first), uint32_t(second));
}

NodePathHash NodePath::calculateHash() const
{
    return sipHash128(ptr, len);
}

NodePathWithHash NodePath::withCalculatedHash() const
{
    return NodePathWithHash{.path = *this, .hash = calculateHash()};
}

NodePath NodePath::parentPath() const
{
    chassert(depth != 0);
    return NodePath(Coordination::parentNodePath(str()), depth - 1);
}

NodePath NodePath::childPath(std::string_view child_name, std::string & path_buf) const
{
    path_buf.clear();
    path_buf.reserve(len + child_name.size() + (depth > 0));
    path_buf.append(str());
    if (depth == 0)
        chassert(str() == "/");
    if (depth > 0)
        path_buf.push_back('/');
    path_buf.append(child_name);
    return NodePath(path_buf, depth + 1);
}

std::string_view NodePath::baseName() const
{
    chassert(depth != 0);
    return Coordination::getBaseNodeName(str());
}

NodePathHash FullNode::getOrCalculatePathHash()
{
    if (path_hash == 0)
        path_hash = path.calculateHash();
    return path_hash;
}

NodePathWithHash FullNode::getPathWithHash()
{
    return {path, getOrCalculatePathHash()};
}

void destroyBlockData(BlockData * ptr) noexcept
{
    ptr->~BlockData();
    ::operator delete(static_cast<void *>(ptr));
}

BlockPtr BlockData::create(size_t capacity_)
{
    if (capacity_ > std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BlockData capacity {} is too large", capacity_);

    /// Allocate the BlockData together with its trailing data buffer, so that reads have one fewer
    /// pointer to chase. The buffer isn't allowed to grow anyway because NodeRef-s must stay valid.
    /// (The control block is allocated separately so that a lingering BlockWeakPtr keeps only the
    /// small control block alive, not the whole BlockData+buffer.)
    void * raw = ::operator new(sizeof(BlockData) + capacity_);
    BlockData * block = new (raw) BlockData();
    block->capacity = static_cast<uint32_t>(capacity_);

    try
    {
        auto * control = new BlockPtrControlBlock{.ptr = block}; // strong = 1, weak = 1
        return BlockPtr(block, control); // adopt the initial strong ref
    }
    catch (...)
    {
        destroyBlockData(block);
        throw;
    }
}

BlockPtr BlockData::copyAndShrinkToFit() const
{
    BlockPtr new_block = create(size);
    uint32_t cap = new_block->capacity;
    memcpy(new_block->data(), data(), size);
    *new_block = *this; // copy all the fields
    new_block->capacity = cap;
    return new_block;
}

void BlockData::readHeader()
{
    /// serialization_version is stored in the file header, not per block, and assigned by the caller.
    if (serialization_version == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper block has invalid serialization version 0");

    if (size < BLOCK_HEADER_SCALARS_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper block is too small to contain a header");

    const char * p = data();
    uint32_t header_size = 0;
    memcpy(&header_size, p, sizeof(header_size));
    p += sizeof(header_size);
    memcpy(&base_depth, p, sizeof(base_depth));
    p += sizeof(base_depth);
    memcpy(&base_path_len, p, sizeof(base_path_len));
    p += sizeof(base_path_len);
    memcpy(&base_zxid, p, sizeof(base_zxid));
    p += sizeof(base_zxid);
    memcpy(&base_time, p, sizeof(base_time));
    p += sizeof(base_time);

    if (size_t(header_size) < BLOCK_HEADER_SCALARS_SIZE + size_t(base_path_len))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "header_size too small");

    base_path_offset = BLOCK_HEADER_SCALARS_SIZE;
    entries_start = header_size;

    if (entries_start > size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper block header extends past the block end");
}

void BlockData::writeHeader(BlockPtr & block, const FullNode & node)
{
    chassert(block->size == 0);
    chassert(block->serialization_version <= SERIALIZATION_VERSION_LATEST);
    if (block->serialization_version == 0)
        block->serialization_version = SERIALIZATION_VERSION_LATEST;

    const uint32_t base_path_len = node.path.len;
    const size_t header_size_64 = BLOCK_HEADER_SCALARS_SIZE + base_path_len;
    if (header_size_64 > UINT32_MAX)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block header size doesn't fit in 32 bits");
    uint32_t header_size = uint32_t(header_size_64);
    reserve(block, header_size_64);

    block->base_depth = node.path.depth;
    block->base_zxid = static_cast<uint64_t>(node.stats.mzxid);
    block->base_time = static_cast<uint64_t>(node.stats.mtime);
    block->base_path_len = base_path_len;
    block->base_path_offset = BLOCK_HEADER_SCALARS_SIZE;
    block->entries_start = header_size;

    char * p = block->data();
    memcpy(p, &header_size, sizeof(header_size));
    p += sizeof(header_size);
    memcpy(p, &block->base_depth, sizeof(block->base_depth));
    p += sizeof(block->base_depth);
    memcpy(p, &base_path_len, sizeof(base_path_len));
    p += sizeof(base_path_len);
    memcpy(p, &block->base_zxid, sizeof(block->base_zxid));
    p += sizeof(block->base_zxid);
    memcpy(p, &block->base_time, sizeof(block->base_time));
    p += sizeof(block->base_time);
    memcpy(p, node.path.ptr, base_path_len);

    block->size = block->entries_start;
}

size_t BlockData::nodeSerializedSizeUpperBound(const FullNode & node) const
{
    /// 2 fixed header bytes + the four group varints at their maximum size + 8 digest bytes.
    /// The path suffix is at most the full path; we don't try to predict prefix compression.
    constexpr size_t constant = 2 + 3 * DB::GroupVarint4x32::MAX_SIZE + DB::GroupVarint8x64::MAX_SIZE + 8;
    return constant + node.path.len + node.stats.data_size;
}

NodeRef BlockData::appendNode(BlockPtr & block, FullNode & node)
{
    if (block->size == 0)
        writeHeader(block, node);

    const size_t upper_bound = block->nodeSerializedSizeUpperBound(node);
    reserve(block, static_cast<size_t>(block->size) + upper_bound);

    return appendNodeNoResize(block, node);
}

NodeRef BlockData::appendNodeNoResize(BlockPtr block, FullNode & node)
{
    /// Serialization format:
    ///    1 byte: NodeAction
    ///    1 byte: varints_size; total size of all the next group varints, up to `path suffix`;
    ///            note that the worst-case total size of these varints is 118 bytes < 256 bytes;
    ///            if bigger than actual varints size, reader ignores the extra bytes; this can be
    ///            used by future versions of the format to add fields that can be ignored by
    ///            older readers
    ///    group varint (4 x u32): path_suffix_size, data_size, acl_id, version
    ///    group varint (4 x u32): path_prefix_size, path_depth_delta, num_children_or_special
    ///    group varint (4 x u32): cversion, aversion
    ///    group varint (8 x u64): czxid_delta, mzxid_delta, pzxid_delta, ctime_delta, mtime_delta,
    ///                            ephemeral_or_seq_num_or_ttl
    ///    path_suffix (path_suffix_size bytes)
    ///    data (data_size bytes)
    ///    8 bytes: digest
    ///
    /// Tombstones (NodeAction::Remove) don't store stats, so the last two group varints are
    /// omitted for them. The digest bytes are kept as padding (see Appendix 1 below).
    ///
    /// The *_delta values are delta+zigzag-encoded relative to a corresponding base_* values in
    /// BlockData.
    /// TODO: Try going overboard and conditionally encoding them relative to each other instead,
    ///       e.g. mtime relative to ctime if version == 0, otherwise relative to base_time.
    ///
    /// The znode path is base_path[:path_prefix_size] + path_suffix.
    ///
    /// It is important that the 8-byte digest comes after all group varints. Explained in Appendix 1 below.
    ///
    /// Path depth is stored explicitly instead of being recalculated from path string. There's no
    /// strong reason for this, it just feels less sketchy when sorting key doesn't rely on
    /// nontrivial calculation and its determinism. We're not planning on changing path string
    /// syntax, but who knows.
    ///
    /// Appendix 1: group varint padding
    /// There's an annoying gotcha: group varint decoder reads (and ignores) up to 8 bytes past
    /// the end. Avoiding this makes it much slower. So we must ensure there are at least
    /// 8 readable bytes of after the last group varint.
    /// Normally this kind of problem would be trivially solved by padding the memory buffer; we'd
    /// read a few bytes into the next znode or into unused buffer space, and that would be fine.
    /// But our situation is different: it's possible that the next znode is being concurrently
    /// serialized by another thread (appending to the block). Then reading+ignoring the first bytes
    /// of the next znode would technically be a data race and UB. It would probably trip TSAN.
    /// So the serialized node must contain at least 8 bytes after the last group varint.
    /// Thankfully we have 8-byte digest to store anyway, so we put it after all the varints.
    /// Make sure to not add more group varints after it, and keep serialized digest at least
    /// 8 bytes (or add other padding)!

    chassert(block->entries_start != 0); // writeHeader must have been called
    const size_t upper_bound = block->nodeSerializedSizeUpperBound(node);
    chassert(static_cast<size_t>(block->size) + upper_bound <= block->capacity);

    const uint32_t node_offset = block->size;
    char * const start = block->data() + node_offset;
    char * p = start;

    /// Fixed-size fields.
    *p = char(uint8_t(node.action));
    ++p;
    char * const varints_size_byte = p++; // filled in at the end

    char * const varints_begin = p;

    /// Path prefix shared with the block's base path.
    const char * const base_path_str = block->data() + block->base_path_offset;
    const uint32_t max_prefix = std::min(block->base_path_len, node.path.len);
    uint32_t path_prefix_size = 0;
    while (size_t(path_prefix_size) + 8 <= size_t(max_prefix) &&
           unalignedLoad<uint64_t>(base_path_str + path_prefix_size) == unalignedLoad<uint64_t>(node.path.ptr + path_prefix_size))
        path_prefix_size += 8;
    while (path_prefix_size < max_prefix && base_path_str[path_prefix_size] == node.path.ptr[path_prefix_size])
        ++path_prefix_size;
    const uint32_t path_suffix_size = node.path.len - path_prefix_size;
    const char * const path_suffix_str = node.path.ptr + path_prefix_size;

    const uint32_t data_size = node.stats.data_size;
    const uint32_t path_depth_delta = encodeZigZagDelta32(block->base_depth, node.path.depth);

    DB::GroupVarint4x32::encode(p, path_suffix_size, data_size, node.stats.acl_id, static_cast<uint32_t>(node.stats.version));
    /// (`num_children_or_special` can take values `UINT32_MAX` and `UINT32_MAX - 1` for ephemeral
    ///  or ttl nodes. Not ideal as it uses 4 bytes. But such nodes are rare, so it's ok.
    ///  We could write `num_children_or_special - SPECIAL_MIN`, so that special values become
    ///  0 and 1; but then the common case of `num_children_or_special = 0` goes from 0 bytes
    ///  to 1 byte, which probably more than outweighs the rare 3-byte savings. We could go weirder,
    ///  like `(num_children_or_special - SPECIAL_MIN) ^ 2`, but that doesn't seem worth the complexity.)
    DB::GroupVarint4x32::encode(p, path_prefix_size, path_depth_delta, node.stats.num_children_or_special, 0);

    uint64_t digest = 0x0000deadbeef0000; // for tombstones the digest bytes are just padding
    if (node.action != NodeAction::Remove)
    {
        DB::GroupVarint4x32::encode(
            p, static_cast<uint32_t>(node.stats.cversion), static_cast<uint32_t>(node.stats.aversion), 0, 0);
        DB::GroupVarint8x64::encode(
            p,
            encodeZigZagDelta64(block->base_zxid, static_cast<uint64_t>(node.stats.czxid)),
            encodeZigZagDelta64(block->base_zxid, static_cast<uint64_t>(node.stats.mzxid)),
            encodeZigZagDelta64(block->base_zxid, static_cast<uint64_t>(node.stats.pzxid)),
            encodeZigZagDelta64(block->base_time, static_cast<uint64_t>(node.stats.ctime)),
            encodeZigZagDelta64(block->base_time, static_cast<uint64_t>(node.stats.mtime)),
            static_cast<uint64_t>(node.stats.ephemeral_or_seq_num_or_ttl),
            0,
            0);

        digest = node.getOrCalculateDigest();
    }

    const size_t varints_size = static_cast<size_t>(p - varints_begin);
    chassert(varints_size <= std::numeric_limits<uint8_t>::max());

    memcpy(p, path_suffix_str, path_suffix_size);
    p += path_suffix_size;
    memcpy(p, node.data_ptr, data_size);
    p += data_size;
    memcpy(p, &digest, 8);
    p += 8;

    *varints_size_byte = static_cast<char>(static_cast<uint8_t>(varints_size));

    const size_t serialized_size = static_cast<size_t>(p - start);
    chassert(serialized_size <= upper_bound);
    block->size = static_cast<uint32_t>(node_offset + serialized_size);

    return NodeRef{.action = node.action, .offset = node_offset, .block = std::move(block)};
}

bool BlockData::appendNodeOrStartNewBlock(const BlockPtr & block, FullNode & node, size_t new_block_capacity, BlockPtr & out_new_block, NodeRef & out_node_ref)
{
    if (block)
    {
        size_t bytes_required = block->nodeSerializedSizeUpperBound(node);
        if (block->capacity - block->size >= bytes_required)
        {
            out_node_ref = appendNodeNoResize(block, node);
            return false;
        }
    }

    out_new_block = BlockData::create(new_block_capacity);
    out_node_ref = BlockData::appendNode(out_new_block, node);
    return true;
}

/// Deserializes a subset of a serialized znode, as requested by the non-null out_* arguments.
/// Serialization format is described in BlockData::appendNodeNoResize.
///
/// If out_path is given, out_path_buf must be given too; the full path is written to out_path_buf
/// (overwriting its previous contents) and out_path points into it.
/// *out_data_ptr points directly into the block's memory.
///
/// (This function has lots of arguments and is full of `if (out_foo != nullptr) { *out_foo = ...; }`
///  branches. That may look needlessly slow, but ALWAYS_INLINE forces it to be inlined into each
///  caller below, where the nullness of every arg is a compile-time constant, so the dead branches
///  get eliminated and each caller ends up like a hand-written partial reader.)
/// (Plain `inline` was not enough for clang.)
/// (Alternatively, we could make it a template and use `if constexpr`, but that seems annoying.)
inline void ALWAYS_INLINE readNodeImpl(
    const BlockData * block, uint32_t offset, DB::KeeperNodeStats * out_stats, const char ** out_data_ptr, NodePath * out_path,
    std::string * out_path_buf, uint64_t * out_digest, NodeAction * out_action, uint32_t * out_serialized_size)
{
    const char * p = block->data() + offset;

    uint8_t action_byte = static_cast<uint8_t>(p[0]);
    if (action_byte > static_cast<uint8_t>(NodeAction::Create))
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Invalid NodeAction: {}", action_byte);
    const NodeAction action = static_cast<NodeAction>(action_byte);
    if (out_action)
        *out_action = action;

    const uint32_t varints_size = static_cast<uint8_t>(p[1]);

    const char * varints = p + 2;
    /// `path suffix` immediately follows all the group varints.
    const char * path_suffix = varints + varints_size;

    /// group varint (4 x u32): path_suffix_size, data_size, acl_id, version
    uint32_t path_suffix_size = 0;
    uint32_t data_size = 0;
    uint32_t acl_id = 0;
    uint32_t version = 0;
    DB::GroupVarint4x32::decode(varints, path_suffix_size, data_size, acl_id, version);

    /// Check against capacity rather than size: capacity is immutable, so this doesn't race with
    /// a thread concurrently appending to the block (reading `size` here would). For blocks read
    /// from files (which may be corrupted) capacity is tight, so the check is just as precise;
    /// for in-memory blocks (well-formed by construction) it's effectively an assert anyway.
    size_t total_size = size_t(2) + varints_size + path_suffix_size + data_size + 8;
    if (total_size > block->capacity - offset)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Node goes out of bounds of its block");

    if (out_serialized_size)
        *out_serialized_size = uint32_t(total_size);

    if (out_stats)
    {
        out_stats->data_size = data_size;
        out_stats->acl_id = acl_id;
        out_stats->version = static_cast<int32_t>(version);
    }

    /// data_ptr is available right after this first group varint; the rest of the fields need more
    /// varints, decoded below.
    if (out_data_ptr)
        *out_data_ptr = path_suffix + path_suffix_size;

    if (out_stats || out_path)
    {
        /// group varint (4 x u32): path_prefix_size, path_depth_delta, num_children_or_special, (unused)
        uint32_t path_prefix_size = 0;
        uint32_t path_depth_delta = 0;
        uint32_t num_children_or_special = 0;
        uint32_t unused = 0;
        DB::GroupVarint4x32::decode(varints, path_prefix_size, path_depth_delta, num_children_or_special, unused);

        if (out_stats)
            out_stats->num_children_or_special = num_children_or_special;

        if (out_stats && action != NodeAction::Remove)
        {
            /// group varint (4 x u32): cversion, aversion, (unused), (unused)
            uint32_t cversion = 0;
            uint32_t aversion = 0;
            DB::GroupVarint4x32::decode(varints, cversion, aversion, unused, unused);
            out_stats->cversion = static_cast<int32_t>(cversion);
            out_stats->aversion = static_cast<int32_t>(aversion);

            /// group varint (8 x u64): czxid_delta, mzxid_delta, pzxid_delta, ctime_delta, mtime_delta,
            ///                         ephemeral_or_seq_num_or_ttl, (unused), (unused)
            uint64_t czxid_delta = 0;
            uint64_t mzxid_delta = 0;
            uint64_t pzxid_delta = 0;
            uint64_t ctime_delta = 0;
            uint64_t mtime_delta = 0;
            uint64_t ephemeral_or_seq_num_or_ttl = 0;
            uint64_t unused64 = 0;
            DB::GroupVarint8x64::decode(
                varints, czxid_delta, mzxid_delta, pzxid_delta, ctime_delta, mtime_delta, ephemeral_or_seq_num_or_ttl, unused64, unused64);

            out_stats->czxid = static_cast<int64_t>(decodeZigZagDelta64(block->base_zxid, czxid_delta));
            out_stats->mzxid = static_cast<int64_t>(decodeZigZagDelta64(block->base_zxid, mzxid_delta));
            out_stats->pzxid = static_cast<int64_t>(decodeZigZagDelta64(block->base_zxid, pzxid_delta));
            out_stats->ctime = static_cast<int64_t>(decodeZigZagDelta64(block->base_time, ctime_delta));
            out_stats->mtime = static_cast<int64_t>(decodeZigZagDelta64(block->base_time, mtime_delta));
            out_stats->ephemeral_or_seq_num_or_ttl = static_cast<int64_t>(ephemeral_or_seq_num_or_ttl);
        }

        /// We do this range check only after doing the reads, instead of expensively pre-checking
        /// before each varint decode. So when deserializing a corrupted file we may read memory
        /// past end of buffer. This is ok; possible outcomes are:
        ///  * one of the bad reads segfaults (unlikely; if undesired, we could pad the buffer on allocation),
        ///  * we get here, fail this check, and discard all results of the bad reads.
        /// Both are fine.
        if (varints > path_suffix)
            throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "Node has incorrect varints_size");

        if (out_path)
        {
            chassert(out_path_buf);
            /// path = base_path[:path_prefix_size] + path_suffix
            out_path_buf->reserve(path_prefix_size + path_suffix_size);
            out_path_buf->assign(block->data() + block->base_path_offset, path_prefix_size);
            out_path_buf->append(path_suffix, path_suffix_size);
            out_path->depth = decodeZigZagDelta32(block->base_depth, path_depth_delta);
            out_path->len = path_prefix_size + path_suffix_size;
            out_path->ptr = out_path_buf->data();
        }
    }

    if (out_digest)
    {
        memcpy(out_digest, path_suffix + path_suffix_size + data_size, 8);
        if (!block->compatible_digest || action == NodeAction::Remove)
            *out_digest = 0;
    }
}

void NodeRef::read(FullNode & out_node, std::string & out_path_buf) const
{
    readNodeImpl(block.get(), offset, &out_node.stats, &out_node.data_ptr, &out_node.path, &out_path_buf, &out_node.digest, &out_node.action, &out_node.serialized_size);
    out_node.path_hash = 0;
}

void NodeRef::readWithoutPath(FullNode & out_node) const
{
    readNodeImpl(block.get(), offset, &out_node.stats, &out_node.data_ptr, nullptr, nullptr, &out_node.digest, &out_node.action, &out_node.serialized_size);
}

void NodeRef::readWithKnownPath(FullNode & out_node, NodePath path) const
{
    readWithoutPath(out_node);
    out_node.path = path;
    out_node.path_hash = 0;
}

void NodeRef::readWithKnownPath(FullNode & out_node, NodePathWithHash path) const
{
    readWithoutPath(out_node);
    out_node.path = path.path;
    out_node.path_hash = path.hash;
}

void NodeRef::readPath(NodePath & out_path, std::string & out_path_buf, uint32_t & out_serialized_size, NodeAction & out_action) const
{
    readNodeImpl(block.get(), offset, nullptr, nullptr, &out_path, &out_path_buf, nullptr, &out_action, &out_serialized_size);
}

uint32_t NodeRef::readSerializedSize() const
{
    uint32_t serialized_size = 0;
    readNodeImpl(block.get(), offset, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, &serialized_size);
    return serialized_size;
}

uint64_t FullNode::getOrCalculateDigest()
{
    if (action == NodeAction::Remove)
        return 0;
    if (digest == 0)
        digest = stats.calculateDigest(path.str(), getData());
    return digest;
}

}
