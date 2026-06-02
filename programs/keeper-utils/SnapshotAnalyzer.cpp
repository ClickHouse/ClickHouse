#include "SnapshotAnalyzer.h"

#include <array>
#include <bit>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Coordination/ACLMap.h>
#include <Coordination/CompactChildrenSet.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/randomSeed.h>

#include <libnuraft/nuraft.hxx>
#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SNAPSHOT;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{

/// Statistics gathered by streaming a single snapshot file.
struct SnapshotStats
{
    SnapshotVersion version = SnapshotVersion::V0;
    uint64_t last_log_idx = 0;
    uint64_t last_log_term = 0;
    int64_t zxid = 0;
    bool has_digest = false;
    uint64_t nodes_digest = 0;
    int64_t session_id_counter = 0;

    uint64_t uncompressed_size = 0;

    size_t num_nodes = 0;
    size_t num_ephemeral = 0;
    size_t num_nonempty_data = 0;
    uint64_t total_data_size = 0;
    uint64_t max_data_size = 0;
    uint64_t sum_path_lengths = 0;
    uint64_t sum_last_component_lengths = 0;
    int32_t max_num_children = 0;
    /// Buckets for number of children: [0, 1, 2, 3, 4, >4].
    std::array<size_t, 6> children_histogram{};

    /// Total heap bytes of the per-node children flat_hash_set-s (only nodes with >1 child have one).
    uint64_t children_set_bytes = 0;
    /// Sum of path lengths of ephemeral nodes (their paths are duplicated in committed_ephemerals).
    uint64_t sum_ephemeral_path_lengths = 0;

    /// ACL map is stored separately only since V1.
    bool acl_map_present = false;
    size_t acl_map_size = 0;
    size_t num_nonempty_acl = 0;

    size_t num_sessions = 0;

    /// Reservoir sample of (path, data size) chosen uniformly at random.
    std::vector<std::pair<std::string, uint64_t>> sample;
};

std::unique_ptr<ReadBuffer> openSnapshotFile(const std::string & path)
{
    std::unique_ptr<ReadBuffer> buf = std::make_unique<ReadBufferFromFile>(path);
    if (path.ends_with(".zstd"))
        buf = wrapReadBufferWithCompressionMethod(std::move(buf), CompressionMethod::Zstd);
    return buf;
}

/// Estimate heap memory used by CompactChildrenSet.
uint64_t childrenSetHeapBytes(int32_t num_children)
{
    if (num_children <= 1)
        return 0;
    auto n = static_cast<uint64_t>(num_children);

    /// This is how much absl::flat_hash_set::reserve(n) actually allocates.
    uint64_t lower_bound = n + (n - 1) / 7;
    uint64_t capacity = std::bit_ceil(lower_bound + 1) - 1;

    return sizeof(ChildrenSet) + capacity * sizeof(std::string_view);
}

/// Stream a snapshot, replicating the on-disk format read sequence from
/// KeeperStorageSnapshot<Storage>::deserialize and readNode in
/// src/Coordination/KeeperSnapshotManager.cpp. Unlike that code, this neither builds the storage
/// nor copies node data (it skips it with ReadBuffer::ignore), so it runs in O(1) memory.
///
/// IMPORTANT: keep this in sync with the snapshot format in KeeperSnapshotManager.cpp.
///
/// If `paths_out` is not null, every path is appended to it (used for the subtree analysis,
/// the only non-O(1)-memory feature).
void calculateSnapshotStats(ReadBuffer & in, size_t sample_size, std::vector<std::string> * paths_out, SnapshotStats & stats)
{
    uint8_t version = 0;
    readBinary(version, in);
    if (version > MAX_SUPPORTED_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);
    stats.version = static_cast<SnapshotVersion>(version);

    /// Snapshot metadata (a small nuraft-serialized blob).
    {
        size_t meta_size = 0;
        readVarUInt(meta_size, in);
        auto buffer = nuraft::buffer::alloc(meta_size);
        in.readStrict(reinterpret_cast<char *>(buffer->data_begin()), meta_size);
        buffer->pos(0);
        auto meta = SnapshotMetadata::deserialize(*buffer);
        stats.last_log_idx = meta->get_last_log_idx();
        stats.last_log_term = meta->get_last_log_term();
    }

    if (stats.version >= SnapshotVersion::V5)
    {
        readBinary(stats.zxid, in);
        uint8_t digest_version = 0;
        readBinary(digest_version, in);
        if (digest_version != static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST))
        {
            readBinary(stats.nodes_digest, in);
            stats.has_digest = true;
        }
    }

    readBinary(stats.session_id_counter, in);

    /// ACL map (stored separately only since V1). Remember which ids have a non-empty ACL list
    /// so we can later count nodes referencing them. The number of distinct ACLs is tiny.
    std::unordered_set<ACLId> nonempty_acl_ids;
    if (stats.version >= SnapshotVersion::V1)
    {
        stats.acl_map_present = true;
        readBinary(stats.acl_map_size, in);
        for (size_t i = 0; i < stats.acl_map_size; ++i)
        {
            ACLId acl_id = 0;
            if (stats.version >= SnapshotVersion::V7)
            {
                readBinary(acl_id, in);
            }
            else
            {
                uint64_t acl_id_64 = 0;
                readBinary(acl_id_64, in);
                acl_id = static_cast<ACLId>(acl_id_64);
            }

            size_t acls_size = 0;
            readBinary(acls_size, in);
            if (acls_size != 0)
                nonempty_acl_ids.insert(acl_id);
            for (size_t j = 0; j < acls_size; ++j)
            {
                int32_t permissions = 0;
                String scheme;
                String id;
                readBinary(permissions, in);
                readBinary(scheme, in);
                readBinary(id, in);
            }
        }
    }

    /// Data tree.
    readBinary(stats.num_nodes, in);

    pcg64 rng(randomSeed());
    if (sample_size != 0)
        stats.sample.reserve(sample_size);
    std::string path;

    for (size_t node_idx = 0; node_idx < stats.num_nodes; ++node_idx)
    {
        size_t path_size = 0;
        readVarUInt(path_size, in);
        path.resize(path_size);
        in.readStrict(path.data(), path_size);

        stats.sum_path_lengths += path_size;
        auto last_slash = path.find_last_of('/');
        stats.sum_last_component_lengths += last_slash == std::string::npos ? path_size : path_size - last_slash - 1;

        /// Node data: we only need its size, so skip the bytes instead of copying them.
        uint64_t data_size = 0;
        readVarUInt(data_size, in);
        if (data_size != 0)
            in.ignore(data_size);

        stats.total_data_size += data_size;
        stats.max_data_size = std::max(stats.max_data_size, data_size);
        if (data_size != 0)
            ++stats.num_nonempty_data;

        /// ACL.
        if (stats.version >= SnapshotVersion::V7)
        {
            ACLId acl_id = 0;
            readBinary(acl_id, in);
            if (nonempty_acl_ids.contains(acl_id))
                ++stats.num_nonempty_acl;
        }
        else if (stats.version >= SnapshotVersion::V1)
        {
            uint64_t acl_id_64 = 0;
            readBinary(acl_id_64, in);
            if (acl_id_64 == std::numeric_limits<uint64_t>::max())
                acl_id_64 = 0;
            if (nonempty_acl_ids.contains(static_cast<ACLId>(acl_id_64)))
                ++stats.num_nonempty_acl;
        }
        else /// V0 stored the ACL list inline in the node.
        {
            size_t acls_size = 0;
            readBinary(acls_size, in);
            if (acls_size != 0)
                ++stats.num_nonempty_acl;
            for (size_t j = 0; j < acls_size; ++j)
            {
                int32_t permissions = 0;
                String scheme;
                String id;
                readBinary(permissions, in);
                readBinary(scheme, in);
                readBinary(id, in);
            }
        }

        if (stats.version < SnapshotVersion::V6)
        {
            bool is_sequential = false;
            readBinary(is_sequential, in);
        }

        int64_t czxid = 0;
        int64_t mzxid = 0;
        int64_t ctime = 0;
        int64_t mtime = 0;
        int32_t node_version = 0;
        int32_t cversion = 0;
        int32_t aversion = 0;
        int64_t ephemeral_owner = 0;
        readBinary(czxid, in);
        readBinary(mzxid, in);
        readBinary(ctime, in);
        readBinary(mtime, in);
        readBinary(node_version, in);
        readBinary(cversion, in);
        readBinary(aversion, in);
        readBinary(ephemeral_owner, in);

        if (stats.version < SnapshotVersion::V6)
        {
            int32_t data_length = 0;
            readBinary(data_length, in);
        }

        int32_t num_children = 0;
        readBinary(num_children, in);

        int64_t pzxid = 0;
        readBinary(pzxid, in);

        if (stats.version >= SnapshotVersion::V7)
        {
            int64_t seq_num = 0;
            readBinary(seq_num, in);
        }
        else
        {
            int32_t seq_num = 0;
            readBinary(seq_num, in);
        }

        if (stats.version >= SnapshotVersion::V4 && stats.version <= SnapshotVersion::V5)
        {
            uint64_t size_bytes = 0;
            readBinary(size_bytes, in);
        }

        if (ephemeral_owner != 0)
        {
            ++stats.num_ephemeral;
            stats.sum_ephemeral_path_lengths += path_size;
        }
        stats.max_num_children = std::max(stats.max_num_children, num_children);
        stats.children_set_bytes += childrenSetHeapBytes(num_children);
        size_t bucket = num_children <= 0 ? 0 : (num_children > 4 ? 5 : static_cast<size_t>(num_children));
        ++stats.children_histogram[bucket];

        /// Reservoir sampling (Algorithm R): keep a uniform sample of size `sample_size`.
        if (sample_size != 0)
        {
            if (stats.sample.size() < sample_size)
            {
                stats.sample.emplace_back(path, data_size);
            }
            else
            {
                size_t j = rng() % (node_idx + 1);
                if (j < sample_size)
                    stats.sample[j] = {path, data_size};
            }
        }

        if (paths_out != nullptr)
            paths_out->push_back(path);

        if ((node_idx + 1) % 10'000'000 == 0)
            std::cerr << fmt::format("Processed {} / {} nodes\n", node_idx + 1, stats.num_nodes);
    }

    /// Sessions.
    readBinary(stats.num_sessions, in);
    for (size_t i = 0; i < stats.num_sessions; ++i)
    {
        int64_t session_id = 0;
        int64_t timeout = 0;
        readBinary(session_id, in);
        readBinary(timeout, in);

        if (stats.version >= SnapshotVersion::V1)
        {
            size_t session_auths_size = 0;
            readBinary(session_auths_size, in);
            for (size_t j = 0; j < session_auths_size; ++j)
            {
                String scheme;
                String id;
                readBinary(scheme, in);
                readBinary(id, in);
            }
        }
    }

    /// Optional cluster config, then drain the rest to learn the total uncompressed size.
    if (!in.eof())
    {
        size_t data_size = 0;
        readVarUInt(data_size, in);
        in.ignore(data_size);
    }
    in.ignoreAll();
    stats.uncompressed_size = in.count();
}

void printSubtrees(const std::vector<std::string> & paths, size_t subtrees_limit)
{
    std::cout << "Finding biggest subtrees... " << std::endl;
    std::unordered_map<std::string_view, size_t> subtree_sizes;
    for (const auto & path : paths)
    {
        if (path == "/")
            continue;

        std::string_view current_path = path;
        while (true)
        {
            auto parent = Coordination::parentNodePath(current_path);
            if (parent == "/") // We are at the root
                break;

            subtree_sizes[parent]++;
            current_path = parent;
        }
    }

    using NodeCount = std::pair<size_t, std::string_view>;
    auto cmp = [](const NodeCount & a, const NodeCount & b) { return a.first > b.first; };
    std::priority_queue<NodeCount, std::vector<NodeCount>, decltype(cmp)> pq(cmp);

    for (const auto & [node_path, count] : subtree_sizes)
    {
        pq.emplace(count, node_path);
        if (pq.size() > subtrees_limit)
            pq.pop();
    }

    std::vector<NodeCount> top_nodes;
    while (!pq.empty())
    {
        top_nodes.push_back(pq.top());
        pq.pop();
    }
    std::reverse(top_nodes.begin(), top_nodes.end());

    std::cout << fmt::format("  Top {} biggest subtrees:\n", subtrees_limit);
    for (const auto & node : top_nodes)
        std::cout << fmt::format("    {}: {} descendants\n", node.second, node.first);
}

void printStats(const SnapshotStats & stats)
{
    const auto avg = [&](uint64_t sum) -> double { return stats.num_nodes == 0 ? 0.0 : static_cast<double>(sum) / static_cast<double>(stats.num_nodes); };

    std::cout << fmt::format("  Last committed log index: {}\n", stats.last_log_idx);
    std::cout << fmt::format("  Last committed log term: {}\n", stats.last_log_term);
    std::cout << fmt::format("  Snapshot format version: {}\n", static_cast<int>(stats.version));
    std::cout << fmt::format("  Uncompressed snapshot size: {} bytes\n", stats.uncompressed_size);
    std::cout << fmt::format("  Number of nodes: {}\n", stats.num_nodes);
    std::cout << fmt::format("  Ephemeral nodes: {}\n", stats.num_ephemeral);
    std::cout << fmt::format("  Digest: {}\n", stats.nodes_digest);
    std::cout << fmt::format("  ZXID: {}\n", stats.zxid);
    std::cout << fmt::format("  Session ID counter: {}\n", stats.session_id_counter);
    std::cout << fmt::format("  Nodes with non-empty data: {}\n", stats.num_nonempty_data);
    std::cout << fmt::format("  Total node data size: {} bytes (avg {:.1f})\n", stats.total_data_size, avg(stats.total_data_size));
    std::cout << fmt::format("  Max node data size: {} bytes\n", stats.max_data_size);
    std::cout << fmt::format("  Sum of path lengths: {} (avg {:.1f})\n", stats.sum_path_lengths, avg(stats.sum_path_lengths));
    std::cout << fmt::format(
        "  Sum of last-component lengths: {} (avg {:.1f})\n", stats.sum_last_component_lengths, avg(stats.sum_last_component_lengths));
    std::cout << fmt::format("  Max number of children: {}\n", stats.max_num_children);
    std::cout << fmt::format(
        "  Nodes by children count:\n    0: {}\n    1: {}\n    2: {}\n    3: {}\n    4: {}\n    >4: {}\n",
        stats.children_histogram[0],
        stats.children_histogram[1],
        stats.children_histogram[2],
        stats.children_histogram[3],
        stats.children_histogram[4],
        stats.children_histogram[5]);
    if (stats.acl_map_present)
        std::cout << fmt::format("  ACL map size: {}\n", stats.acl_map_size);
    else
        std::cout << "  ACL map size: n/a (pre-V1 snapshot, ACLs stored inline)\n";
    std::cout << fmt::format("  Nodes with non-empty ACL: {}\n", stats.num_nonempty_acl);
    std::cout << fmt::format("  Number of sessions: {}\n", stats.num_sessions);

    std::cout << fmt::format("  Random sample of up to {} nodes (path: data size):\n", stats.sample.size());
    for (const auto & [path, data_size] : stats.sample)
        std::cout << fmt::format("    {}: {} bytes\n", path, data_size);
}

/// A breakdown of the predicted memory usage of KeeperMemoryStorage after loading this snapshot.
struct MemoryEstimate
{
    uint64_t node_structs = 0;   /// KeeperMemNode objects
    uint64_t list_overhead = 0;  /// ListNode wrapper (key view + metadata) + std::list prev/next
    uint64_t path_keys = 0;      /// arena-owned path strings (keys)
    uint64_t node_data = 0;      /// arena-owned node data buffers
    uint64_t children_sets = 0;  /// per-node children flat_hash_set-s (nodes with >1 child)
    uint64_t index_map = 0;      /// SnapshotableHashTable index HashMap buckets
    uint64_t ephemerals = 0;     /// committed_ephemerals (approximate)
    uint64_t sessions = 0;       /// session_and_timeout + committed_session_and_auth (approximate)
    uint64_t total = 0;
};

/// In practice this underestimated by ~20%, we haven't investigated why.
MemoryEstimate estimateKeeperStorageMemory(const SnapshotStats & stats)
{
    /// Sizes verified against the codebase:
    ///   sizeof(KeeperMemNode) == 104            (static_assert in KeeperStorage.h)
    ///   sizeof(ListNode<KeeperMemNode>) == 128  (static_assert in SnapshotableHashTable.h)
    ///   sizeof(HashMapCell<string_view, list::iterator>) == 24
    static constexpr uint64_t node_struct_size = 104;
    static constexpr uint64_t list_node_size = 128;
    static constexpr uint64_t list_ptr_overhead = 2 * sizeof(void *); /// std::list prev/next pointers
    static constexpr uint64_t index_cell_size = 24;

    MemoryEstimate m;
    const auto n = static_cast<uint64_t>(stats.num_nodes);

    /// Nodes live in a std::list<ListNode<KeeperMemNode>>. Report the KeeperMemNode struct itself
    /// separately from the ListNode wrapper (key string_view + version metadata) and the list pointers.
    m.node_structs = n * node_struct_size;
    m.list_overhead = n * (list_node_size - node_struct_size + list_ptr_overhead);

    /// Paths (keys) and node data are owned as exact-size char[] in the container's arena.
    m.path_keys = stats.sum_path_lengths;
    m.node_data = stats.total_data_size;

    /// Per-node children flat_hash_set-s, accumulated exactly during the scan.
    m.children_sets = stats.children_set_bytes;

    /// Index HashMap<string_view, list::iterator>: a flat power-of-two array of cells, resized at a
    /// 50% max load factor, so bufSize is the smallest 2^k >= 2 * num_nodes (initial size is 256).
    if (n != 0)
        m.index_map = std::max<uint64_t>(256, std::bit_ceil(2 * n)) * index_cell_size;

    /// Approximate: ephemeral paths are duplicated as std::string in committed_ephemerals
    /// (unordered_map<session, unordered_set<string>>): ~one hash-set node + bucket slot per node,
    /// plus the path bytes themselves (those that exceed the std::string small-string buffer).
    m.ephemerals
        = stats.num_ephemeral * (sizeof(std::string) + 16 /*hash node header*/ + 8 /*bucket slot*/) + stats.sum_ephemeral_path_lengths;

    /// Approximate: session_and_timeout + committed_session_and_auth, ~one unordered_map node each
    /// (auth strings, usually absent, are not counted).
    m.sessions = stats.num_sessions * 2 * (sizeof(std::pair<int64_t, int64_t>) + 16 /*hash node header*/ + 8 /*bucket slot*/);

    m.total = m.node_structs + m.list_overhead + m.path_keys + m.node_data + m.children_sets + m.index_map + m.ephemerals + m.sessions;
    return m;
}

void printMemoryEstimate(const SnapshotStats & stats)
{
    auto m = estimateKeeperStorageMemory(stats);
    const auto line = [](const std::string & label, uint64_t bytes)
    {
        std::cout << fmt::format("    {:<40}{:>16} bytes ({:.1f} MiB)\n", label, bytes, static_cast<double>(bytes) / (1024.0 * 1024.0));
    };

    std::cout << "  Predicted KeeperMemoryStorage memory usage (not very accurate, expect ~20% underestimate):\n";
    line("KeeperMemNode (104 B):", m.node_structs);
    line("ListNode and std::list overhead (40 B):", m.list_overhead);
    line("Paths:", m.path_keys);
    line("Node data:", m.node_data);
    line("Children flat_hash_sets (>1 child):", m.children_sets);
    line("Index HashMap:", m.index_map);
    line("Ephemerals:", m.ephemerals);
    line("Sessions:", m.sessions);
    line("Total:", m.total);
}

void analyzeSingleSnapshot(
    const std::string & full_path, bool with_node_stats, size_t subtrees_limit, size_t sample_size)
{
    auto in = openSnapshotFile(full_path);

    SnapshotStats stats;
    std::vector<std::string> paths;
    calculateSnapshotStats(*in, sample_size, with_node_stats ? &paths : nullptr, stats);

    printStats(stats);
    printMemoryEstimate(stats);

    if (with_node_stats)
        printSubtrees(paths, subtrees_limit);

    std::cout << std::endl;
}

}

void analyzeSnapshot(
    const std::string & snapshot_path, bool with_node_stats, size_t subtrees_limit, size_t sample_size)
{
    try
    {
        std::vector<std::string> snapshot_paths;
        bool specific_snapshot_defined = snapshot_path.ends_with(".bin") || snapshot_path.ends_with(".bin.zstd");

        if (specific_snapshot_defined)
        {
            snapshot_paths.push_back(snapshot_path);
        }
        else
        {
            for (const auto & entry : std::filesystem::directory_iterator(snapshot_path))
            {
                if (!entry.is_regular_file())
                    continue;
                auto name = entry.path().filename().string();
                if (name.starts_with("snapshot_") && (name.ends_with(".bin") || name.ends_with(".bin.zstd")))
                    snapshot_paths.push_back(name);
            }

            if (snapshot_paths.empty())
                throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "No snapshot files found in {}", snapshot_path);

            // Sort snapshots by their index (newest first)
            std::sort(snapshot_paths.begin(), snapshot_paths.end(), std::greater<>());

            std::cout << "Found " << snapshot_paths.size() << " snapshots in " << snapshot_path << ":\n\n";
        }

        for (const auto & snapshot_file : snapshot_paths)
        {
            try
            {
                std::string full_path
                    = specific_snapshot_defined ? snapshot_path : (std::filesystem::path(snapshot_path) / snapshot_file).generic_string();
                std::cout << "=== Snapshot: " << snapshot_file << " ===\n";

                analyzeSingleSnapshot(full_path, with_node_stats, subtrees_limit, sample_size);
            }
            catch (const Exception & e)
            {
                std::cerr << "  Error analyzing snapshot " << snapshot_file << ": " << e.message() << "\n\n";
            }
        }
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Failed to analyze snapshots in {}: {}", snapshot_path, e.message());
    }
}

}
