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
    extern const CoordinationSettingsUInt64 write_throttling_min_delay_us;
    extern const CoordinationSettingsUInt64 write_throttling_max_delay_us;
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
    try
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

        const NodePathHash hash = node.getOrCalculatePathHash();
        auto * lookup = node_cache.map.find(hash);
        std::optional<NodeAction> combined;

        /// Validate `action` before mutating anything.
        if (lookup)
        {
            /// The node already exists, so its history so far combines to Create.
            /// Combine that with the new action, strictly (e.g. asserts we don't Create it again).
            combined = combineActions(NodeAction::Create, node.action, /*strict=*/ true);
            chassert(!combined || combined == NodeAction::Create);
        }
        else
        {
            if (node.action != NodeAction::Create)
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR, "Unexpected NodeAction {} for a node that doesn't exist",
                    uint32_t(node.action));
        }

        const NodeRef ref = mutable_memtable->appendNode(node, /*strict=*/ true);

        /// Update `node_cache`. (We hold storage_mutex exclusively, so no concurrent readers;
        /// no need for the per-entry spinlocks.)
        if (lookup)
        {
            if (!combined)
            {
                /// Create + Remove: `node_cache` doesn't keep removed nodes.
                node_cache.map.erase(hash);
            }
            else
            {
                NodeRefCache::Entry & info = lookup->getMapped();
                info.file_seqno = mutable_memtable->file_seqno;
                info.block.store(ref.block);
                info.node_offset = ref.offset;
            }
        }
        else
        {
            NodeRefCache::Entry & info = node_cache.map[hash];
            info.file_seqno = mutable_memtable->file_seqno;
            info.block.store(ref.block);
            info.node_offset = ref.offset;
        }

        return ref;
    }
    catch (...)
    {
        /// Maybe MEMORY_LIMIT_EXCEEDED is possible here. We currently don't handle it, and the
        /// caller doesn't handle it.
        DB::tryLogCurrentException(log, "Unexpected exception");
        std::abort();
    }
}

void StorageState::listCommittedChildrenNames(
    const NodePathWithHash & path, ChildrenSet2 & out, DB::Arena & arena) const
{
    /// Visit memtables and sorted runs from newest to oldest, recording the first (newest)
    /// occurrence of each child name in `out`. This includes tombstones; e.g. if the latest
    /// memtable removed a child, its listChildrenNames will insert an action=Remove into `out`,
    /// then listChildrenNames in older memtables and files won't insert this child into `out`.

    if (mutable_memtable)
        mutable_memtable->listChildrenNames(path, out, arena);
    for (auto it = immutable_memtables.rbegin(); it != immutable_memtables.rend(); ++it)
        (*it)->listChildrenNames(path, out, arena);

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
            (*it)->listChildrenNames(range_start, range_end, out, arena, block_cache.get());
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
    try
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
    catch (...)
    {
        /// Maybe MEMORY_LIMIT_EXCEEDED is possible here. We currently don't handle it, and the
        /// caller doesn't handle it.
        DB::tryLogCurrentException(log, "Unexpected exception");
        std::abort();
    }
}

void StorageState::cleanupUncommittedState(int64_t committed_zxid)
{
    while (!uncommitted.empty() && uncommitted.front().max_zxid <= committed_zxid)
    {
        LOG_DEBUG(log, "Removing obsolete uncommitted memtable with max_zxid = {} (committed_zxid = {})", uncommitted.front().max_zxid, committed_zxid);

        uncommitted.erase(uncommitted.begin());
    }
}

void StorageState::listUncommittedChildrenNames(
    const NodePathWithHash & path, ChildrenSet2 & out, DB::Arena & arena) const
{
    for (auto it = uncommitted.rbegin(); it != uncommitted.rend(); ++it)
        it->memtable->listChildrenNames(path, out, arena);

    {
        std::shared_lock lock(*storage_mutex);
        listCommittedChildrenNames(path, out, arena);
    }
}

void StorageState::throttleWrite() const
{
    int64_t delay_us = write_throttling_us.load(std::memory_order_relaxed);
    if (delay_us != 0)
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
}

void StorageState::recalculateWriteThrottling()
{
    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    bool limit_reached = false;
    double excess = 0.0;

    auto consider = [&](size_t current, size_t limit)
    {
        if (current < limit)
            return;
        limit_reached = true;
        excess += double(current) / std::max(1.0, double(limit)) - 1.0;
    };

    consider(immutable_memtables.size(), settings[DB::CoordinationSetting::unflushed_memtables_soft_limit]);
    consider(sorted_runs.size(), settings[DB::CoordinationSetting::sorted_runs_soft_limit]);

    int64_t delay_us = 0;
    if (limit_reached)
    {
        const uint64_t max_delay_us = settings[DB::CoordinationSetting::write_throttling_max_delay_us];
        const uint64_t min_delay_us = settings[DB::CoordinationSetting::write_throttling_min_delay_us];
        double factor = double(settings[DB::CoordinationSetting::write_throttling_factor]);
        factor = std::max(1.0, factor);

        /// Exponential backoff, clamped to max_delay_us. If `excess` is very large, pow overflows
        /// to +inf, and min() clamps it back to max_delay_us, so delay stays finite.
        delay_us = int64_t(std::min(double(max_delay_us), double(min_delay_us) * std::pow(factor, excess)));
    }

    int64_t prev = write_throttling_us.exchange(delay_us);

    if (delay_us != prev)
        LOG_INFO(log, "{} writes, there are {} immutable memtables and {} sorted runs", delay_us ? "Throttling" : "Unthrottling", immutable_memtables.size(), sorted_runs.size());
}

}
