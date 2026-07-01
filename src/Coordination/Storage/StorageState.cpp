#include <Coordination/Storage/StorageState.h>

#include <Coordination/Storage/BackgroundWork.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <shared_mutex>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 committed_memtable_size;
    extern const CoordinationSettingsUInt64 memtable_block_size;
    extern const CoordinationSettingsUInt64 uncommitted_memtable_size;
    extern const CoordinationSettingsUInt64 unflushed_memtables_soft_limit;
    extern const CoordinationSettingsUInt64 sorted_runs_soft_limit;
    extern const CoordinationSettingsUInt64 write_throttling_min_delay_ms;
    extern const CoordinationSettingsUInt64 write_throttling_max_delay_ms;
    extern const CoordinationSettingsFloat write_throttling_factor;
}

namespace Coordination::Storage
{

StorageState::StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_)
    : keeper_context(std::move(keeper_context_)), log(getLogger("KeeperLSMT")), storage_mutex(storage_mutex_)
{
    /// TODO: Init memory_only. Init block_cache if not memory_only.
}

StorageState::~StorageState()
{
    shutdown();
}

void StorageState::startup()
{
    chassert(!background);
    background = std::make_unique<BackgroundWork>(this);
}

void StorageState::shutdown()
{
    if (background)
    {
        background->shutdown();
        background.reset();
    }
}

NodeRef StorageState::getCommittedNode(const NodePathWithHash & path) const
{
    const NodeRefCache::Entry * info = nullptr;
    NodeRef node_ref;
    if (node_cache.tryGet(path.hash, node_ref, &info))
        /// Normal fast path: the node is already in memory
        /// (in memtable, or in block cache, or pinned by SortedFile in memory-only mode).
        return node_ref;
    if (!info)
        return NodeRef{}; // the node is not in NodeCache's map and therefore doesn't exist

    /// The block was evicted from the block cache. Memtables keep their blocks alive, so the
    /// node's latest update must be in a file (sorted run).
    ///
    /// We can't binary-search the run by file_seqno: while a merge incrementally publishes its
    /// output run alongside the not-yet-consumed suffixes of its input runs, several runs cover
    /// overlapping seqno ranges. So scan runs newest-first; among the runs whose seqno range covers
    /// our seqno, exactly one actually has the path (its cutoff lets the others reject it).
    const uint32_t seqno = info->file_seqno;
    BlockPtr block;
    const SortedRun * found_run = nullptr;
    for (auto it = sorted_runs.rbegin(); it != sorted_runs.rend(); ++it)
    {
        const SortedRun & run = **it;
        if (seqno < run.min_file_seqno || seqno > run.max_file_seqno)
            continue;
        block = run.getBlockCoveringPath(path.path, block_cache.get());
        if (block)
        {
            found_run = &run;
            break;
        }
    }
    if (!block)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Node's block expired, but no sorted run covering file_seqno {} has its path {}", seqno, path.path.str());
    const SortedRun & run = *found_run;

    /// Re-point `node_cache` entries at the freshly loaded copy of the block, for all nodes
    /// in it. We may be holding storage_mutex in shared mode, so concurrent readers may be doing
    /// the same; that's fine: we only update existing entries (no map rehash), one entry at a time
    /// under its spinlock.
    NodeRef ref{.action = NodeAction::Create, .offset = 0, .block = block};
    std::string path_buf;
    for (uint32_t offset = block->entries_start; offset < block->size;)
    {
        ref.offset = offset;
        NodePath node_path;
        uint32_t serialized_size = 0;
        NodeAction action = NodeAction::Remove;
        ref.readPath(node_path, path_buf, serialized_size, action);

        if (const auto * node_lookup = node_cache.map.find(node_path.calculateHash()))
        {
            const NodeRefCache::Entry & node_info = node_lookup->getMapped();
            /// Don't touch entries whose latest update is in a newer sorted run or memtable.
            if (run.max_file_seqno >= node_info.file_seqno)
            {
                chassert(run.min_file_seqno <= node_info.file_seqno);
                std::lock_guard guard(node_info.block);
                node_info.block.set(block);
                node_info.node_offset = offset;
            }
        }

        offset += serialized_size;
    }

    /// `info` is still valid: the loop above only updated existing `node_cache` entries.
    std::lock_guard guard(info->block);
    BlockPtr loaded = info->block.get();
    if (!loaded)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Node (file_seqno {}) not found in the expected block of sorted run covering {}-{}",
            seqno, run.min_file_seqno, run.max_file_seqno);
    return NodeRef{.action = NodeAction::Create, .offset = info->node_offset, .block = std::move(loaded)};
}

NodeRef StorageState::getUncommittedNode(const NodePathWithHash & path)
{
    /// Search uncommitted memtables, newest first. The found NodeRef may be a tombstone
    /// (action == Remove) with a non-null block.
    for (auto it = uncommitted.rbegin(); it != uncommitted.rend(); ++it)
        if (const auto * lookup = it->nodes.find(path.hash))
            return lookup->getMapped();

    std::shared_lock lock(*storage_mutex);
    return getCommittedNode(path);
}

NodeRef StorageState::appendCommittedNode(FullNode & node)
{
    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    if (!mutable_memtable ||
        /// (Quirk: this condition will usually pass just after allocating a new block in the memtable.
        ///  So we'll usually finalize the memtable with a nearly empty last block, wasting its capacity.
        ///  That's fine, memtable usually has lots of blocks, this is a tiny waste of memory.)
        mutable_memtable->total_bytes > settings[DB::CoordinationSetting::committed_memtable_size])
    {
        if (mutable_memtable)
        {
            immutable_memtables.push_back(std::move(mutable_memtable));
            recalculateWriteThrottling();
            background->maybeStartFlush();
        }

        mutable_memtable = std::make_shared<Memtable>();
        mutable_memtable->target_block_size = settings[DB::CoordinationSetting::memtable_block_size];
        mutable_memtable->file_seqno = next_file_seqno++;

        LOG_DEBUG(log, "Creating new memtable {}", mutable_memtable->file_seqno);

        /// TODO: Create block_cache (if not memory-only mode) or update its settings if changed.
    }

    const NodeRef ref = mutable_memtable->appendNode(node, /*strict=*/ true);

    /// Update `node_cache`. (We hold storage_mutex exclusively, so no concurrent readers;
    /// no need for the per-entry spinlocks.)
    const NodePathHash hash = node.getOrCalculatePathHash();
    if (auto * lookup = node_cache.map.find(hash))
    {
        NodeRefCache::Entry & info = lookup->getMapped();
        /// The node already exists, so its history so far combines to Create.
        /// Combine that with the new action, strictly (e.g. asserts we don't Create it again).
        std::optional<NodeAction> combined = combineActions(NodeAction::Create, node.action, /*strict=*/ true);
        if (!combined)
        {
            /// Create + Remove: `node_cache` doesn't keep removed nodes.
            node_cache.map.erase(hash);
        }
        else
        {
            chassert(*combined == NodeAction::Create); // Create + Update = Create
            info.file_seqno = mutable_memtable->file_seqno;
            info.block.store(ref.block);
            info.node_offset = ref.offset;
        }
    }
    else
    {
        if (node.action != NodeAction::Create)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Unexpected NodeAction {} for a node that doesn't exist",
                uint32_t(node.action));

        NodeRefCache::Entry & info = node_cache.map[hash];
        info.file_seqno = mutable_memtable->file_seqno;
        info.block.store(ref.block);
        info.node_offset = ref.offset;
    }

    return ref;
}

void StorageState::visitCommittedChildren(
    const NodePathWithHash & path, bool full_node,
    const std::function<bool(std::string_view /*name*/, const NodeRef &, const FullNode *)> & check_node) const
{
    DB::Arena arena;
    ChildrenSet2 seen;
    visitCommittedChildren(path, full_node, seen, arena, check_node);
}

void StorageState::visitCommittedChildren(
    const NodePathWithHash & path, bool full_node, ChildrenSet2 & seen, DB::Arena & arena,
    const std::function<bool(std::string_view /*name*/, const NodeRef &, const FullNode *)> & check_node) const
{
    /// Visit memtables and sorted runs from newest to oldest, recording the first (newest)
    /// occurrence of each child name in `seen`.

    /// Memtables don't get whole nodes as byproduct of listing children, and they don't even have
    /// means of finding a node by path. Give them a callback to help out with that.
    std::function<NodeRef(const NodePathWithHash &)> load_node;
    if (full_node)
    {
        load_node = [&](const NodePathWithHash & child_path)
        {
            NodeRef ref;
            bool found = node_cache.tryGet(child_path.hash, ref);
            chassert(found);
            return ref;
        };
    }

    if (mutable_memtable)
    {
        if (!mutable_memtable->visitChildren(path, load_node, check_node, seen, arena))
            return;
    }
    for (auto it = immutable_memtables.rbegin(); it != immutable_memtables.rend(); ++it)
    {
        if (!(*it)->visitChildren(path, load_node, check_node, seen, arena))
            return;
    }

    if (!sorted_runs.empty())
    {
        /// The direct children of `path` are exactly the nodes Q with range_start < Q < range_end (both
        /// bounds exclusive), at depth path.depth + 1, where:
        ///   range_start = path + "/"   (e.g. "/foo/bar/")
        ///   range_end   = range_start with the last char bumped from '/' to '0' ('/'+1)   (e.g. "/foo/bar0")
        /// In the (depth, path string) order this is exactly the depth-(path.depth+1) nodes whose string
        /// starts with the "path + '/'" prefix.
        std::string range_start_str(path.path.str());
        if (!range_start_str.ends_with('/'))
            range_start_str += '/';
        std::string range_end_str = range_start_str;
        ++range_end_str.back(); // '/' (0x2F) -> '0' (0x30)
        const NodePath range_start(range_start_str, path.path.depth + 1);
        const NodePath range_end(range_end_str, path.path.depth + 1);

        for (auto it = sorted_runs.rbegin(); it != sorted_runs.rend(); ++it)
        {
            if (!(*it)->visitChildren(range_start, range_end, full_node, check_node, seen, arena, block_cache.get()))
                return;
        }
    }
}

void StorageState::getNodeCountAndDataSize(uint64_t & out_node_count, uint64_t & out_data_size) const
{
    out_data_size = 0;
    int64_t node_count = 0;
    auto visit_memtable = [&](const Memtable & m)
    {
        node_count += m.node_count_delta;
        out_data_size += m.total_bytes;
    };
    if (mutable_memtable)
        visit_memtable(*mutable_memtable);
    for (const auto & m : immutable_memtables)
        visit_memtable(*m);
    for (const auto & r : sorted_runs)
    {
        node_count += r->node_count_delta;
        out_data_size += r->total_block_size;
    }
    chassert(node_count >= 0);
    out_node_count = static_cast<uint64_t>(node_count);
}

NodeRef StorageState::appendUncommittedNode(FullNode & node, int64_t zxid)
{
    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    if (uncommitted.empty()
        || uncommitted.back().memtable->total_bytes > settings[DB::CoordinationSetting::uncommitted_memtable_size])
    {
        if (!uncommitted.empty())
            LOG_DEBUG(log, "Creating new uncommitted memtable (last memtable max_zxid = {}, current zxid = {})", uncommitted.back().max_zxid, zxid);

        UncommittedMemtable u;
        u.memtable = std::make_shared<Memtable>();
        u.memtable->target_block_size = settings[DB::CoordinationSetting::memtable_block_size];
        uncommitted.push_back(std::move(u));
    }

    UncommittedMemtable & u = uncommitted.back();
    u.max_zxid = std::max(u.max_zxid, zxid);
    /// strict=false: see the comment at Memtable::appendNode.
    NodeRef ref = u.memtable->appendNode(node, /*strict=*/ false);
    /// Loose model: the last record for a path wins, including Remove tombstones.
    u.nodes[node.getOrCalculatePathHash()] = ref;
    return ref;
}

void StorageState::cleanupUncommittedState(int64_t committed_zxid)
{
    while (!uncommitted.empty() && uncommitted.front().max_zxid <= committed_zxid)
    {
        LOG_DEBUG(log, "Removing obsolete uncommitted memtable with max_zxid = {} (committed_zxid = {})", uncommitted.front().max_zxid, committed_zxid);

        uncommitted.erase(uncommitted.begin());
    }
}

void StorageState::visitUncommittedChildren(
    const NodePathWithHash & path, bool full_node,
    const std::function<bool(std::string_view /*name*/, const NodeRef &, const FullNode *)> & check_node) const
{
    DB::Arena arena;
    ChildrenSet2 seen;

    for (auto it = uncommitted.rbegin(); it != uncommitted.rend(); ++it)
    {
        std::function<NodeRef(const NodePathWithHash &)> load_node;
        if (full_node)
        {
            load_node = [&](const NodePathWithHash & child_path)
            {
                /// The latest child info may be in a later memtable than the child-name entry,
                /// because Update action doesn't touch Memtable::children. So we look at all
                /// memtables here, not just `it`.
                for (auto inner_it = uncommitted.rbegin();; ++inner_it)
                {
                    const auto * lookup = inner_it->nodes.find(child_path.hash);
                    if (lookup)
                    {
                        NodeRef ref = lookup->getMapped();
                        chassert(ref); // not a tombstone
                        return ref;
                    }
                    /// Node must be present in memtable `it`, if nowhere else.
                    chassert(inner_it != it);
                }
            };
        }

        if (!it->memtable->visitChildren(path, load_node, check_node, seen, arena))
            return;
    }

    {
        std::shared_lock lock(*storage_mutex);
        visitCommittedChildren(path, full_node, seen, arena, check_node);
    }
}

void StorageState::throttleWrite() const
{
    size_t amount = write_throttling.load(std::memory_order_relaxed);
    if (amount == 0)
        return;

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();
    const uint64_t max_delay_ms = settings[DB::CoordinationSetting::write_throttling_max_delay_ms];
    const uint64_t min_delay_ms = settings[DB::CoordinationSetting::write_throttling_min_delay_ms];
    const float factor = settings[DB::CoordinationSetting::write_throttling_factor];

    /// Exponential backoff, clamped to max_delay_ms. If `amount` is very large, pow overflows to
    /// +inf, and min() clamps it back to max_delay_ms, so delay_ms stays finite.
    const double delay_ms = std::min(double(max_delay_ms), double(min_delay_ms) * std::pow(double(factor), double(amount)));
    std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int64_t>(delay_ms)));
}

void StorageState::recalculateWriteThrottling()
{
    auto excess = [](size_t current, size_t limit) { return current - std::min(current, limit); };

    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();
    size_t flushes_fell_behind = excess(immutable_memtables.size(), settings[DB::CoordinationSetting::unflushed_memtables_soft_limit]);
    size_t merges_fell_behind = excess(sorted_runs.size(), settings[DB::CoordinationSetting::sorted_runs_soft_limit]);

    /// TODO: Change this to add *relative* excesses: max(0, flushes / max_flushes - 1) + max(merges / max_merges - 1).
    ///       Having 15/5 memtables in memory is much worse than 110/100 sorted runs on disk.
    size_t throttle = flushes_fell_behind + merges_fell_behind;
    size_t prev = write_throttling.exchange(throttle);

    if (throttle != prev)
        LOG_INFO(log, "{} writes, there are {} immutable memtables and {} sorted runs", throttle ? "Throttling" : "Unthrottling", immutable_memtables.size(), sorted_runs.size());
}

}
