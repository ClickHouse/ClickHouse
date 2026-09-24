#include <gtest/gtest.h>

#include <Coordination/Storage/Node.h>
#include <Common/Exception.h>

#include <cstring>
#include <string>


namespace Coordination::Storage
{

namespace
{

using DB::ErrorCodes;

/// Appends a hand-crafted serialized node entry to the block and returns a NodeRef to it.
/// Entry layout (see BlockData::appendNodeNoResize):
///   [action][varints_size][gv4: path_suffix_size, data_size, acl_id, version]
///   [gv4: path_prefix_size, path_depth_delta, num_children, flags][path_suffix][8-byte digest]
NodeRef appendCraftedEntry(BlockPtr & block, NodeAction action, uint32_t path_suffix_size, uint32_t path_prefix_size)
{
    char entry[64] = {};
    char * p = entry;
    *p++ = static_cast<char>(static_cast<uint8_t>(action));
    char * varints_size_byte = p++;

    char * varints_begin = p;
    DB::GroupVarint4x32::encode(p, path_suffix_size, 0, 0, 0);
    DB::GroupVarint4x32::encode(p, path_prefix_size, 0, 0, 0);
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

FullNode makeNode(const std::string & path, const std::string & data)
{
    FullNode node;
    node.action = NodeAction::Create;
    node.path = NodePath(path);
    node.setData(data);
    return node;
}

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

/// A corrupted entry claiming a path_prefix_size larger than the block's base path must be
/// rejected instead of copying that many bytes out of the block buffer.
/// With path_prefix_size = 1024 and a 64-byte block the copy runs far past the allocation.
TEST(KeeperStorageNode, ReadRejectsOversizedPathPrefix)
{
    BlockPtr block = BlockData::create(64);

    FullNode base = makeNode("/ab", "");
    BlockData::appendNode(block, base);

    NodeRef ref = appendCraftedEntry(block, NodeAction::Create, /*path_suffix_size=*/ 1, /*path_prefix_size=*/ 1024);

    FullNode node;
    std::string path_buf;
    try
    {
        ref.read(node, path_buf);
        FAIL() << "Expected Exception for path_prefix_size > base_path_len";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA);
    }
}

/// Same check for a value that stays inside the block capacity: without validation such an
/// entry silently copies unrelated block bytes into the decoded path (heap data leak).
TEST(KeeperStorageNode, ReadRejectsPathPrefixExceedingBasePathWithinCapacity)
{
    BlockPtr block = BlockData::create(256);

    FullNode base = makeNode("/ab", "");
    BlockData::appendNode(block, base);

    NodeRef ref = appendCraftedEntry(block, NodeAction::Create, /*path_suffix_size=*/ 1, /*path_prefix_size=*/ 100);

    FullNode node;
    std::string path_buf;
    try
    {
        ref.read(node, path_buf);
        FAIL() << "Expected Exception for path_prefix_size > base_path_len";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA);
    }
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
