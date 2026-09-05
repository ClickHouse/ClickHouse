#include <SnapshotAnalyzer.h>

#include <algorithm>
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
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperMemNodesStorage.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SNAPSHOT;
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
    uint64_t nodes_digest = 0;
    int64_t session_id_counter = 0;

    uint64_t uncompressed_size = 0;

    size_t num_nodes = 0;
    size_t num_ephemeral = 0;
    size_t num_ttl = 0;
    size_t num_container = 0;
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
    /// Nodes with the most children, sorted by children count descending.
    std::vector<std::pair<std::string, int32_t>> top_children;
};

/// Streaming tracker of the K nodes with the most children.
/// Candidates not better than the K-th best seen so far are rejected with a single comparison.
/// The rest go into a buffer of capacity 2K that is compacted back to K elements with quickselect
/// whenever it fills up, so adding a node takes amortized O(1) time and the memory usage is O(K).
class TopChildrenTracker
{
public:
    explicit TopChildrenTracker(size_t limit_) : limit(limit_) { buffer.reserve(2 * limit); }

    void add(std::string_view path, int32_t num_children)
    {
        if (limit == 0 || num_children <= threshold)
            return;
        buffer.emplace_back(path, num_children);
        if (buffer.size() >= 2 * limit)
        {
            /// Quickselect the K-th largest; everything after it can't make the top K
            /// (ties are broken arbitrarily).
            std::nth_element(buffer.begin(), buffer.begin() + limit - 1, buffer.end(), byChildrenDesc);
            buffer.resize(limit);
            threshold = buffer.back().second;
        }
    }

    std::vector<std::pair<std::string, int32_t>> finish() &&
    {
        std::sort(buffer.begin(), buffer.end(), byChildrenDesc);
        if (buffer.size() > limit)
            buffer.resize(limit);
        return std::move(buffer);
    }

private:
    static bool byChildrenDesc(const std::pair<std::string, int32_t> & a, const std::pair<std::string, int32_t> & b)
    {
        return a.second > b.second;
    }

    size_t limit;
    /// Children count of the K-th best node seen so far (-1 until the buffer first fills up).
    int32_t threshold = -1;
    std::vector<std::pair<std::string, int32_t>> buffer;
};

std::unique_ptr<ReadBuffer> openSnapshotFile(const std::string & path)
{
    /// Mirror KeeperSnapshotManager: a snapshot is either zstd-compressed (the default,
    /// keeper_server.coordination_settings.compress_snapshots_with_zstd_format=true) or compressed
    /// with ClickHouse's CompressedWriteBuffer otherwise. The file extension is not authoritative
    /// (both formats are written to *.bin / *.bin.zstd depending on the setting), so detect the
    /// format by the leading magic bytes exactly like KeeperSnapshotManager::isZstdCompressed.
    static constexpr unsigned char ZSTD_COMPRESSED_MAGIC[4] = {0x28, 0xB5, 0x2F, 0xFD};

    auto file = std::make_unique<ReadBufferFromFile>(path);

    unsigned char magic[sizeof(ZSTD_COMPRESSED_MAGIC)]{};
    size_t bytes_read = file->read(reinterpret_cast<char *>(magic), sizeof(magic));
    file->seek(0, SEEK_SET);

    if (bytes_read == sizeof(magic) && memcmp(magic, ZSTD_COMPRESSED_MAGIC, sizeof(magic)) == 0)
        return wrapReadBufferWithCompressionMethod(std::move(file), CompressionMethod::Zstd);

    return std::make_unique<CompressedReadBufferFromFile>(std::move(file));
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

/// Stream a snapshot through KeeperSnapshotReader, gathering statistics without building the
/// storage. Node data is read into a small reused buffer, so this runs in O(max node size) memory.
///
/// `raw_in` must be the same ReadBuffer that was passed to (and is owned by) `reader`; it is used
/// to measure the total uncompressed size after everything is read.
///
/// If `paths_out` is not null, every path is appended to it (used for the subtree analysis,
/// the only non-O(1)-memory feature).
void calculateSnapshotStats(
    KeeperSnapshotReader & reader, ReadBuffer & raw_in, size_t sample_size, std::vector<std::string> * paths_out, SnapshotStats & stats)
{
    reader.readMetadata();

    stats.version = reader.current_version;
    stats.last_log_idx = reader.snapshot_meta->get_last_log_idx();
    stats.last_log_term = reader.snapshot_meta->get_last_log_term();
    stats.zxid = reader.commit_zxid;
    stats.nodes_digest = reader.nodes_digest;
    stats.session_id_counter = reader.session_id_counter;

    reader.readACLMapAndNodeCount();
    stats.num_nodes = reader.node_count;

    /// Remember which ACL ids map to a non-empty ACL list so we can count nodes referencing them.
    /// The number of distinct ACLs is tiny. (For V0 snapshots ACLs are stored inline in the nodes
    /// and the reader interns them into acl_map on the fly, assigning acl_id == 0 iff the list is
    /// empty, so this set stays empty and the acl_id != 0 check below suffices.)
    stats.acl_map_present = stats.version >= SnapshotVersion::V1;
    std::unordered_set<ACLId> nonempty_acl_ids;
    for (const auto & [acl_id, acls] : reader.acl_map.getMapping())
    {
        ++stats.acl_map_size;
        if (!acls.empty())
            nonempty_acl_ids.insert(acl_id);
    }

    pcg64 rng(randomSeed());
    if (sample_size != 0)
        stats.sample.reserve(sample_size);
    if (paths_out != nullptr)
        paths_out->reserve(reader.node_count);

    auto streams = reader.createStreams(1);
    auto & stream = *streams.at(0);

    std::string path;
    std::string data;
    KeeperNodeStats node_stats;
    size_t path_size = 0;
    for (size_t node_idx = 0; stream.readNodePathSize(path_size); ++node_idx)
    {
        path.resize(path_size);
        size_t data_size = 0;
        stream.readNodePathAndDataSize(path.data(), path_size, data_size);
        data.resize(data_size);
        stream.readNodeDataAndStats(path, data.data(), data_size, node_stats);

        stats.sum_path_lengths += path_size;
        auto last_slash = path.find_last_of('/');
        stats.sum_last_component_lengths += last_slash == std::string::npos ? path_size : path_size - last_slash - 1;

        stats.total_data_size += data_size;
        stats.max_data_size = std::max<uint64_t>(stats.max_data_size, data_size);
        if (data_size != 0)
            ++stats.num_nonempty_data;

        if (node_stats.acl_id != 0 && (stats.version == SnapshotVersion::V0 || nonempty_acl_ids.contains(node_stats.acl_id)))
            ++stats.num_nonempty_acl;

        if (node_stats.isEphemeral())
        {
            ++stats.num_ephemeral;
            stats.sum_ephemeral_path_lengths += path_size;
        }
        if (node_stats.isTTL())
            ++stats.num_ttl;
        if (node_stats.isContainer())
            ++stats.num_container;

        int32_t num_children = node_stats.getNumChildren();
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
    reader.finishStreams(std::move(streams));

    /// The reader deserializes sessions into a KeeperStorage; give it an empty one just to count
    /// the sessions (a few bytes each, so still effectively O(1) memory).
    auto storage = KeeperStorage::create(
        /* tick_time_ms */ 500, /* superdigest */ "", reader.keeper_context, /* initialize_system_nodes */ false);
    reader.readSessionsAndClusterConfig(*storage);
    stats.num_sessions = storage->session_and_timeout.size();

    /// Drain the rest (normally already at EOF) to learn the total uncompressed size.
    raw_in.ignoreAll();
    stats.uncompressed_size = raw_in.count();
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
    std::cout << fmt::format("  TTL nodes: {}\n", stats.num_ttl);
    std::cout << fmt::format("  Container nodes: {}\n", stats.num_container);
    /// Both fields are only present in newer snapshots; don't print a misleading 0 when they are absent.
    if (stats.nodes_digest != 0)
        std::cout << fmt::format("  Digest: {}\n", stats.nodes_digest);
    else
        std::cout << "  Digest: none (disabled, unsupported digest version, or pre-V5 snapshot)\n";
    if (stats.version >= SnapshotVersion::V5)
        std::cout << fmt::format("  ZXID: {}\n", stats.zxid);
    else
        std::cout << "  ZXID: n/a (pre-V5 snapshot)\n";
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

/// A breakdown of the predicted memory usage of KeeperMemNodesStorage after loading this snapshot.
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

using NodesList = std::list<ListNode<KeeperMemNode>>;

/// KeeperMemNodesStorage keeps nodes in SnapshotableHashTable<KeeperMemNode>:
/// a std::list<ListNode<KeeperMemNode>> indexed by HashMap<string_view, list iterator>.
constexpr uint64_t node_struct_size = sizeof(KeeperMemNode);
constexpr uint64_t list_node_size = sizeof(NodesList::value_type);
constexpr uint64_t list_ptr_overhead = 2 * sizeof(void *); /// std::list prev/next pointers
/// HashMapCell<string_view, list iterator>: key + mapped, no extra fields.
constexpr uint64_t index_cell_size = sizeof(std::string_view) + sizeof(NodesList::iterator);

/// In practice this underestimated by ~20%, we haven't investigated why.
MemoryEstimate estimateKeeperStorageMemory(const SnapshotStats & stats)
{
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

    std::cout << "  Predicted KeeperMemNodesStorage memory usage (not very accurate, expect ~20% underestimate):\n";
    line(fmt::format("KeeperMemNode ({} B):", node_struct_size), m.node_structs);
    line(fmt::format("ListNode and std::list overhead ({} B):", list_node_size - node_struct_size + list_ptr_overhead), m.list_overhead);
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
    auto keeper_context = std::make_shared<KeeperContext>(/* standalone_keeper_ */ true, std::make_shared<CoordinationSettings>());

    auto in = openSnapshotFile(full_path);
    /// The reader takes ownership of `in` and keeps it alive; keep a reference to measure
    /// the uncompressed size at the end.
    ReadBuffer & raw_in = *in;
    KeeperSnapshotReader reader(std::move(in), keeper_context);

    SnapshotStats stats;
    std::vector<std::string> paths;
    calculateSnapshotStats(reader, raw_in, sample_size, with_node_stats ? &paths : nullptr, stats);

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
