#include <gtest/gtest.h>

#include <Coordination/Storage/Node.h>
#include <Common/Exception.h>

#include <cstring>
#include <string>
#include <string_view>


namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
}

namespace Coordination::Storage
{

namespace
{

/// Appends one hand-crafted serialized node entry to the block and returns a NodeRef to it.
/// Entry layout (see BlockData::appendNodeNoResize):
///   [action][varints_size]
///   [gv4: path_suffix_size, data_size, acl_id, version]
///   [gv4: path_prefix_size, path_depth_delta, num_children, flags]
///   [gv4: cversion, aversion, unused, unused]                                    (Create only)
///   [gv8x64: czxid/mzxid/pzxid/ctime/mtime deltas, ephemeral_or_seq_num_or_ttl, unused, unused]  (Create only)
///   [path_suffix][8-byte digest]
NodeRef appendCraftedEntry(BlockPtr & block, NodeAction action, uint32_t path_suffix_size, uint32_t path_prefix_size)
{
    char entry[64] = {};
    char * p = entry;
    *p++ = static_cast<char>(static_cast<uint8_t>(action));
    char * varints_size_byte = p++;

    char * varints_begin = p;
    DB::GroupVarint4x32::encode(p, path_suffix_size, 0, 0, 0);
    DB::GroupVarint4x32::encode(p, path_prefix_size, 0, 0, 0);
    if (action != NodeAction::Remove)
    {
        DB::GroupVarint4x32::encode(p, 0, 0, 0, 0);
        DB::GroupVarint8x64::encode(p, 0, 0, 0, 0, 0, 0, 0, 0);
    }
    *varints_size_byte = static_cast<char>(static_cast<uint8_t>(p - varints_begin));

    memset(p, 'c', path_suffix_size);
    p += path_suffix_size;
    const uint64_t digest = 0;
    memcpy(p, &digest, sizeof(digest));
    p += sizeof(digest);

    const size_t entry_size = static_cast<size_t>(p - entry);
    const uint32_t offset = block->size;
    memcpy(block->data() + block->size, entry, entry_size);
    block->size = static_cast<uint32_t>(block->size + entry_size);

    return NodeRef{.action = action, .offset = offset, .block = block};
}

FullNode makeNode(std::string_view path, std::string_view data)
{
    FullNode node;
    node.action = NodeAction::Create;
    node.path = NodePath(path);
    node.setData(data);
    return node;
}

/// Decodes one crafted Create entry with a valid path_prefix_size.
/// The block capacity is a parameter so that the success case is identical to the
/// matching failure case except for path_prefix_size.
void expectPrefixAccepted(uint32_t path_prefix_size, size_t block_capacity)
{
    BlockPtr block = BlockData::create(block_capacity);
    FullNode base = makeNode("/ab", "");
    BlockData::appendNode(block, base);

    NodeRef ref = appendCraftedEntry(block, NodeAction::Create, /*path_suffix_size=*/ 1, path_prefix_size);
    FullNode node;
    std::string path_buf;
    ref.read(node, path_buf);
    EXPECT_EQ(node.path.str(), "/abc");
    EXPECT_EQ(node.path.len, 4u);
    EXPECT_EQ(node.getData(), "");
    EXPECT_EQ(node.action, NodeAction::Create);
}

/// Decodes one crafted Create entry whose path_prefix_size exceeds the base path length.
void expectPrefixRejected(uint32_t path_prefix_size, size_t block_capacity)
{
    BlockPtr block = BlockData::create(block_capacity);
    FullNode base = makeNode("/ab", "");
    BlockData::appendNode(block, base);

    NodeRef ref = appendCraftedEntry(block, NodeAction::Create, /*path_suffix_size=*/ 1, path_prefix_size);
    FullNode node;
    std::string path_buf;
    try
    {
        ref.read(node, path_buf);
        FAIL() << "Expected Exception for path_prefix_size " << path_prefix_size
               << " > base_path_len 3";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(std::string(e.message()).find("exceeds the base path length"), std::string::npos)
            << "Expected the new path_prefix_size check, got: " << e.message();
    }
}

}

/// The success and failure entries are identical except for path_prefix_size:
/// 3 == base_path_len("/ab") decodes fine, 1024 runs past the end of the 64-byte block.
TEST(KeeperStorageNode, ReadRejectsOversizedPathPrefix)
{
    expectPrefixAccepted(3, /*block_capacity=*/ 64);
    expectPrefixRejected(1024, /*block_capacity=*/ 64);
}

/// Same, but the oversized prefix stays inside the block capacity: without the check it
/// silently copies unrelated block bytes into the decoded path.
TEST(KeeperStorageNode, ReadRejectsPathPrefixExceedingBasePathWithinCapacity)
{
    expectPrefixAccepted(3, /*block_capacity=*/ 256);
    expectPrefixRejected(100, /*block_capacity=*/ 256);
}

/// Legitimate encoding/decoding must keep working after the validation is added.
/// The child path fully shares the base path prefix, so path_prefix_size == base_path_len.
TEST(KeeperStorageNode, AppendAndReadRoundTrip)
{
    BlockPtr block = BlockData::create(4096);

    FullNode base = makeNode("/ab", "");
    BlockData::appendNode(block, base);

    FullNode child = makeNode("/ab/cd", "hello");
    child.stats.version = 3;
    NodeRef ref = BlockData::appendNode(block, child);

    FullNode read_back;
    std::string path_buf;
    ref.read(read_back, path_buf);

    EXPECT_EQ(read_back.action, NodeAction::Create);
    EXPECT_EQ(read_back.path.str(), "/ab/cd");
    EXPECT_EQ(read_back.path.depth, 2u);
    EXPECT_EQ(read_back.getData(), "hello");
    EXPECT_EQ(read_back.stats.version, 3);

    NodePath path;
    uint32_t serialized_size = 0;
    NodeAction action = NodeAction::Remove;
    ref.readPath(path, path_buf, serialized_size, action);
    EXPECT_EQ(path.str(), "/ab/cd");
    EXPECT_EQ(action, NodeAction::Create);
    EXPECT_EQ(serialized_size, ref.readSerializedSize());
}

TEST(KeeperStorageNode, TombstoneRoundTrip)
{
    BlockPtr block = BlockData::create(4096);

    FullNode base = makeNode("/ab", "");
    BlockData::appendNode(block, base);

    FullNode removed;
    removed.action = NodeAction::Remove;
    removed.path = NodePath("/ab/cd");
    NodeRef ref = BlockData::appendNode(block, removed);

    FullNode read_back;
    std::string path_buf;
    ref.read(read_back, path_buf);

    EXPECT_EQ(read_back.action, NodeAction::Remove);
    EXPECT_EQ(read_back.path.str(), "/ab/cd");
    EXPECT_EQ(read_back.stats.data_size, 0u);
    EXPECT_EQ(read_back.getOrCalculateDigest(), 0u);
}

}
