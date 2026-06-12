#include <gtest/gtest.h>

#include <Coordination/Storage/Node.h>
#include <Coordination/Storage/Memtable.h>
#include <Coordination/Storage/NodeStream.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/SharedMutex.h>

#include <fmt/format.h>

#include <chrono>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace Coordination::Storage;

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 committed_memtable_size;
    extern const CoordinationSettingsUInt64 memtable_block_size;
    extern const CoordinationSettingsUInt64 file_block_size;
    extern const CoordinationSettingsUInt64 sorted_file_uncompressed_size;
    extern const CoordinationSettingsUInt64 min_files_to_merge;
    extern const CoordinationSettingsUInt64 max_files_to_merge;
    extern const CoordinationSettingsFloat max_size_ratio;
    extern const CoordinationSettingsUInt64 unflushed_memtables_soft_limit;
    extern const CoordinationSettingsUInt64 sorted_runs_soft_limit;
}

namespace
{

FullNode makeNode(
    NodeAction action,
    uint32_t depth,
    std::string_view path,
    std::string_view data,
    uint32_t acl_id,
    uint32_t version,
    uint32_t num_children,
    bool is_ephemeral,
    DB::KeeperNodeStats stats)
{
    FullNode node;
    node.action = action;
    node.stats = stats; // stat fields (czxid, mzxid, ...); the rest are overwritten below
    node.stats.acl_id = acl_id;
    node.stats.version = static_cast<int32_t>(version);
    node.stats.setNumChildrenAndIsEphemeral(num_children, is_ephemeral);
    node.data_ptr = data.data();
    node.stats.data_size = static_cast<uint32_t>(data.size());
    node.path = NodePath(path, depth);
    return node;
}

/// Ephemeral node with data.
FullNode makeNodeA(std::string_view path)
{
    return makeNode(
        NodeAction::Create, 2, path, "hello world", /*acl_id*/ 1, /*version*/ 3, /*num_children*/ 0, /*is_ephemeral*/ true,
        DB::KeeperNodeStats{
            .czxid = 101,
            .mzxid = 205,
            .pzxid = 210,
            .ctime = 1700000000123,
            .mtime = 1700000111456,
            .cversion = 7,
            .aversion = 2,
            .ephemeral_owner_or_seq_num = 777});
}

/// Non-ephemeral node with a seq num and children, empty data.
FullNode makeNodeB(std::string_view path)
{
    return makeNode(
        NodeAction::Update, 2, path, "", /*acl_id*/ 0, /*version*/ 0, /*num_children*/ 5, /*is_ephemeral*/ false,
        DB::KeeperNodeStats{
            .czxid = 300,
            .mzxid = 300,
            .pzxid = 305,
            .ctime = 1,
            .mtime = 2,
            .cversion = 1,
            .aversion = 0,
            .ephemeral_owner_or_seq_num = 4});
}

/// Root node with all-default stats and 1-byte data.
FullNode makeNodeC()
{
    return makeNode(
        NodeAction::Create, 0, "/", "x", /*acl_id*/ 0, /*version*/ 0, /*num_children*/ 2, /*is_ephemeral*/ false, DB::KeeperNodeStats{});
}

void expectNodesEqual(const FullNode & expected, const FullNode & actual)
{
    EXPECT_EQ(expected.action, actual.action);
    EXPECT_EQ(expected.stats.data_size, actual.stats.data_size);
    EXPECT_EQ(expected.stats.acl_id, actual.stats.acl_id);
    EXPECT_EQ(expected.stats.version, actual.stats.version);
    EXPECT_EQ(expected.stats.getNumChildren(), actual.stats.getNumChildren());
    EXPECT_EQ(expected.stats.isEphemeral(), actual.stats.isEphemeral());

    EXPECT_EQ(expected.stats.czxid, actual.stats.czxid);
    EXPECT_EQ(expected.stats.mzxid, actual.stats.mzxid);
    EXPECT_EQ(expected.stats.pzxid, actual.stats.pzxid);
    EXPECT_EQ(expected.stats.ctime, actual.stats.ctime);
    EXPECT_EQ(expected.stats.mtime, actual.stats.mtime);
    EXPECT_EQ(expected.stats.cversion, actual.stats.cversion);
    EXPECT_EQ(expected.stats.aversion, actual.stats.aversion);
    EXPECT_EQ(expected.stats.ephemeral_owner_or_seq_num, actual.stats.ephemeral_owner_or_seq_num);

    EXPECT_EQ(expected.path.depth, actual.path.depth);
    EXPECT_EQ(expected.path.str(), actual.path.str());
    EXPECT_EQ(expected.getData(), actual.getData());
}

}

TEST(KeeperStorage, NodeSerializationRoundTrip)
{
    const std::string path_a = "/test/digest_a";
    const std::string path_b = "/test/digest_b";
    const std::string path_tomb = "/test/removed";

    FullNode node_a = makeNodeA(path_a);
    FullNode node_b = makeNodeB(path_b);
    FullNode node_c = makeNodeC();
    FullNode tomb;
    tomb.action = NodeAction::Remove;
    tomb.path = NodePath(path_tomb, 2);

    /// Tiny initial capacity, so that appends exercise block reallocation.
    BlockPtr block = BlockData::create(16);
    block->compatible_digest = true;

    std::vector<NodeRef> refs;
    refs.push_back(BlockData::appendNode(block, node_a));
    refs.push_back(BlockData::appendNode(block, node_b));
    refs.push_back(BlockData::appendNode(block, tomb));
    refs.push_back(BlockData::appendNode(block, node_c));

    /// appendNode assigned digests to the (non-tombstone) input nodes.
    EXPECT_NE(node_a.digest, 0);
    EXPECT_NE(node_b.digest, 0);
    EXPECT_NE(node_c.digest, 0);

    FullNode out;
    std::string path_buf;

    refs[0].read(out, path_buf);
    expectNodesEqual(node_a, out);
    EXPECT_EQ(out.digest, node_a.digest);
    EXPECT_EQ(refs[0].offset + out.serialized_size, refs[1].offset);

    refs[1].read(out, path_buf);
    expectNodesEqual(node_b, out);
    EXPECT_EQ(out.digest, node_b.digest);
    EXPECT_EQ(refs[1].offset + out.serialized_size, refs[2].offset);

    /// Tombstones store only the path; stats are not stored for them.
    FullNode tomb_out;
    refs[2].read(tomb_out, path_buf);
    EXPECT_EQ(tomb_out.action, NodeAction::Remove);
    EXPECT_EQ(tomb_out.path.str(), path_tomb);
    EXPECT_EQ(tomb_out.path.depth, 2);
    EXPECT_EQ(tomb_out.stats.data_size, 0);
    EXPECT_EQ(refs[2].offset + tomb_out.serialized_size, refs[3].offset);

    refs[3].read(out, path_buf);
    expectNodesEqual(node_c, out);
    EXPECT_EQ(refs[3].offset + out.serialized_size, block->size);

    /// Partial read: everything except the path.
    FullNode partial;
    refs[0].readWithoutPath(partial);
    EXPECT_EQ(partial.stats.version, node_a.stats.version);
    EXPECT_EQ(partial.getData(), node_a.getData());

    /// Digests of nodes read from a block with !compatible_digest are discarded.
    /// (refs[0].block is not necessarily `block`: appends may have reallocated into a bigger
    ///  block, while refs keep the blocks they were written to alive.)
    refs[0].block->compatible_digest = false;
    refs[0].read(out, path_buf);
    EXPECT_EQ(out.digest, 0);
}

TEST(KeeperStorage, NodeDigestCompatibility)
{
    /// Reference values calculated by KeeperMemNode::getDigest (i.e. calculateDigest in
    /// KeeperStorage.cpp, KeeperDigestVersion::V4) for nodes with the same fields.
    const std::string path_a = "/test/digest_a";
    FullNode node_a = makeNodeA(path_a);
    EXPECT_EQ(node_a.getOrCalculateDigest(), 13507803326533446230ULL);
    EXPECT_EQ(node_a.digest, 13507803326533446230ULL);

    const std::string path_b = "/test/digest_b";
    FullNode node_b = makeNodeB(path_b);
    EXPECT_EQ(node_b.getOrCalculateDigest(), 13631188841398170870ULL);

    FullNode node_c = makeNodeC();
    EXPECT_EQ(node_c.getOrCalculateDigest(), 16444598805986783812ULL);

    /// getOrCalculateDigest doesn't overwrite an already assigned digest.
    node_c.digest = 42;
    EXPECT_EQ(node_c.getOrCalculateDigest(), 42);
    node_c.digest = 0;
    EXPECT_EQ(node_c.getOrCalculateDigest(), 16444598805986783812ULL);
}

/// End-to-end stress of the background flush/merge pipeline: insert a large committed dataset (with
/// removals and a nested subtree) with tiny memtable/file thresholds, so it goes through many flushes
/// and merges that incrementally publish their output and trim their (fully consumed) inputs. Then
/// check that reads and child listings reflect exactly the survivors once it converges to one run.
TEST(KeeperStorage, BackgroundFlushAndMerge)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    /// Tiny thresholds: force frequent flushes, multi-file runs, and partial-publishing merges.
    (*settings)[DB::CoordinationSetting::committed_memtable_size] = 2048;
    (*settings)[DB::CoordinationSetting::memtable_block_size] = 256;
    (*settings)[DB::CoordinationSetting::file_block_size] = 256;
    (*settings)[DB::CoordinationSetting::sorted_file_uncompressed_size] = 512;
    (*settings)[DB::CoordinationSetting::min_files_to_merge] = 2;
    (*settings)[DB::CoordinationSetting::max_files_to_merge] = 8;
    (*settings)[DB::CoordinationSetting::max_size_ratio] = 1.0f; // always merge, so it converges to one run

    auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper*/ true, settings);
    DB::SharedMutex storage_mutex;
    StorageState storage(keeper_context, &storage_mutex);
    storage.memory_only = true;
    storage.startup();

    auto append = [&](NodeAction action, const std::string & path, std::string_view data)
    {
        FullNode node;
        node.action = action;
        node.path = NodePath(path);
        node.data_ptr = data.data();
        node.stats.data_size = static_cast<uint32_t>(data.size());
        storage.appendCommittedNode(node);
    };
    auto numbered = [](std::string_view prefix, int i)
    {
        return fmt::format("{}{:05}", prefix, i);
    };

    constexpr int num_k = 1200; // direct children /a/k*
    constexpr int num_removed = 400; // first num_removed of them get removed
    constexpr int num_c = 600; // grandchildren /a/b/c*

    {
        std::lock_guard lock(storage_mutex);
        append(NodeAction::Create, "/a", "A");
        append(NodeAction::Create, "/a/b", "B");
        for (int i = 0; i < num_k; ++i)
            append(NodeAction::Create, numbered("/a/k", i), "K");
        for (int i = 0; i < num_removed; ++i)
            append(NodeAction::Remove, numbered("/a/k", i), "");
        for (int i = 0; i < num_c; ++i)
            append(NodeAction::Create, numbered("/a/b/c", i), "C");
    }

    /// Wait for background flushes and merges to converge to a single sorted run.
    bool converged = false;
    for (int attempt = 0; attempt < 4000 && !converged; ++attempt)
    {
        {
            std::shared_lock lock(storage_mutex);
            converged = storage.immutable_memtables.empty() && storage.sorted_runs.size() == 1;
        }
        if (!converged)
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    ASSERT_TRUE(converged);

    std::shared_lock lock(storage_mutex);

    /// The dataset is much bigger than sorted_file_uncompressed_size, so the converged run spans many
    /// files - which means merges did incrementally publish output and trim inputs along the way.
    EXPECT_FALSE(storage.sorted_runs[0]->min_path_cutoff.has_value());
    EXPECT_GT(storage.sorted_runs[0]->files.size(), 1u);

    auto count_living_children = [&](const std::string & parent)
    {
        size_t count = 0;
        storage.visitCommittedChildren(
            NodePath(parent).withCalculatedHash(), /*full_node=*/ false,
            [&](std::string_view, const NodeRef &, const FullNode *) { ++count; return true; });
        return count;
    };

    /// Direct children of /a are the surviving /a/k* plus /a/b (grandchildren /a/b/c* are excluded).
    EXPECT_EQ(count_living_children("/a"), size_t(num_k - num_removed + 1));
    EXPECT_EQ(count_living_children("/a/b"), size_t(num_c));

    /// node_count_delta summed across the run and memtables must equal the true node count:
    /// /a + /a/b + surviving /a/k* + /a/b/c* (the removed /a/k* net out to zero).
    int64_t total_node_count_delta = storage.sorted_runs[0]->node_count_delta;
    for (const MemtablePtr & memtable : storage.immutable_memtables)
        total_node_count_delta += memtable->node_count_delta;
    if (storage.mutable_memtable)
        total_node_count_delta += storage.mutable_memtable->node_count_delta;
    EXPECT_EQ(total_node_count_delta, int64_t(2 + (num_k - num_removed) + num_c));

    std::string buf;
    for (int i = 0; i < num_removed; ++i)
        EXPECT_FALSE(bool(storage.getCommittedNode(NodePath(numbered("/a/k", i)).withCalculatedHash())))
            << "removed node " << numbered("/a/k", i) << " is still present";

    for (int i = num_removed; i < num_k; ++i)
    {
        NodeRef ref = storage.getCommittedNode(NodePath(numbered("/a/k", i)).withCalculatedHash());
        ASSERT_TRUE(bool(ref)) << "survivor " << numbered("/a/k", i) << " is missing";
        FullNode out;
        ref.read(out, buf);
        EXPECT_EQ(out.getData(), "K");
    }

    for (int i = 0; i < num_c; ++i)
        EXPECT_TRUE(bool(storage.getCommittedNode(NodePath(numbered("/a/b/c", i)).withCalculatedHash())))
            << "grandchild " << numbered("/a/b/c", i) << " is missing";

    storage.shutdown();
}

/// SnapshotWriterNodeStream emits exactly the set of existing committed nodes, with no NodeAction
/// nonsense. Here everything stays in a single mutable memtable (large committed_memtable_size, no
/// flush), which exercises the memtable phase, including that a tombstone must suppress (not emit)
/// the node and must not let an older Create for the same path resurrect it.
TEST(KeeperStorage, SnapshotWriter)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::committed_memtable_size] = 64 * 1024 * 1024; // never flush
    (*settings)[DB::CoordinationSetting::memtable_block_size] = 256; // several blocks per memtable

    auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper*/ true, settings);
    DB::SharedMutex storage_mutex;
    StorageState storage(keeper_context, &storage_mutex);
    storage.memory_only = true;
    storage.startup();

    auto numbered = [](std::string_view prefix, int i) { return fmt::format("{}{:05}", prefix, i); };

    constexpr int num_n = 300;
    constexpr int num_removed = 100;

    std::set<std::string> expected;
    {
        std::lock_guard lock(storage_mutex);
        auto append = [&](NodeAction action, const std::string & path)
        {
            static constexpr std::string_view data = "v";
            FullNode node;
            node.action = action;
            node.path = NodePath(path);
            node.data_ptr = data.data();
            node.stats.data_size = static_cast<uint32_t>(data.size());
            storage.appendCommittedNode(node);
        };

        append(NodeAction::Create, "/s");
        expected.insert("/s");
        for (int i = 0; i < num_n; ++i)
        {
            append(NodeAction::Create, numbered("/s/n", i));
            expected.insert(numbered("/s/n", i));
        }
        /// Remove the first num_removed nodes (Create + Remove of the same path in one memtable).
        for (int i = 0; i < num_removed; ++i)
        {
            append(NodeAction::Remove, numbered("/s/n", i));
            expected.erase(numbered("/s/n", i));
        }
    }

    std::shared_lock lock(storage_mutex);
    /// Everything stayed in the single mutable memtable (nothing was flushed).
    ASSERT_TRUE(storage.immutable_memtables.empty());
    ASSERT_TRUE(storage.sorted_runs.empty());

    SnapshotWriterNodeStream snapshot(storage);
    EXPECT_EQ(snapshot.getNodeCount(), expected.size());

    std::set<std::string> emitted;
    size_t emitted_count = 0;
    while (true)
    {
        snapshot.next();
        if (snapshot.at_end)
            break;
        EXPECT_TRUE(snapshot.node.action == NodeAction::Create);
        emitted.insert(std::string(snapshot.node.path.str()));
        ++emitted_count;
    }

    EXPECT_EQ(emitted_count, expected.size()); // every node emitted exactly once
    EXPECT_EQ(emitted, expected);

    storage.shutdown();
}

/// recalculateWriteThrottling sets the throttle level to how far past the soft limits the number of
/// unflushed memtables and active sorted runs got. We drive it directly (no background threads).
TEST(KeeperStorage, WriteThrottling)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::unflushed_memtables_soft_limit] = 4;
    (*settings)[DB::CoordinationSetting::sorted_runs_soft_limit] = 3;

    auto keeper_context = std::make_shared<DB::KeeperContext>(/*standalone_keeper*/ true, settings);
    DB::SharedMutex storage_mutex;
    StorageState storage(keeper_context, &storage_mutex);
    /// Deliberately not calling startup(): no background threads touch the state we set up here.

    std::lock_guard lock(storage_mutex);

    /// Within both soft limits: no throttling.
    storage.recalculateWriteThrottling();
    EXPECT_EQ(storage.write_throttling.load(), 0u);

    /// 6 unflushed memtables (2 over the limit of 4) and 5 sorted runs (2 over the limit of 3).
    for (int i = 0; i < 6; ++i)
        storage.immutable_memtables.push_back(std::make_shared<Memtable>());
    for (uint32_t i = 0; i < 5; ++i)
        storage.sorted_runs.push_back(std::make_shared<SortedRun>(i, i));

    storage.recalculateWriteThrottling();
    EXPECT_EQ(storage.write_throttling.load(), size_t(2 + 2));

    /// Only sorted runs over the limit now (4 over the limit of 3).
    storage.immutable_memtables.clear();
    for (uint32_t i = 5; i < 7; ++i)
        storage.sorted_runs.push_back(std::make_shared<SortedRun>(i, i)); // 7 runs total
    storage.recalculateWriteThrottling();
    EXPECT_EQ(storage.write_throttling.load(), size_t(7 - 3));

    /// Back within limits: throttling clears.
    storage.sorted_runs.clear();
    storage.recalculateWriteThrottling();
    EXPECT_EQ(storage.write_throttling.load(), 0u);
}
