#include <Coordination/Storage/StorageState.h>

#include <Coordination/Storage/BackgroundWork.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <base/getMemoryAmount.h>
#include <base/scope_guard.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <Core/Defines.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool storage_memory_only;
    extern const CoordinationSettingsUInt64 block_cache_size;
    extern const CoordinationSettingsFloat block_cache_size_ratio;
    extern const CoordinationSettingsUInt64 committed_memtable_size;
    extern const CoordinationSettingsUInt64 memtable_block_size;
    extern const CoordinationSettingsUInt64 uncommitted_memtable_size;
    extern const CoordinationSettingsUInt64 unflushed_memtables_soft_limit;
    extern const CoordinationSettingsUInt64 sorted_runs_soft_limit;
    extern const CoordinationSettingsUInt64 write_throttling_min_delay_us;
    extern const CoordinationSettingsUInt64 write_throttling_max_delay_us;
    extern const CoordinationSettingsFloat write_throttling_factor;
}

namespace ProfileEvents
{
    extern const Event KeeperLSMTUncommittedCreates;
    extern const Event KeeperLSMTUncommittedCreateBytes;
    extern const Event KeeperLSMTUncommittedUpdates;
    extern const Event KeeperLSMTUncommittedUpdateBytes;
    extern const Event KeeperLSMTUncommittedRemoves;
    extern const Event KeeperLSMTUncommittedRemoveBytes;
    extern const Event KeeperLSMTCommittedEntryBytes;
    extern const Event KeeperLSMTThrottledWrites;
    extern const Event KeeperLSMTCommittedMemtablesCreated;
    extern const Event KeeperLSMTUncommittedMemtablesCreated;
    extern const Event KeeperLSMTGetUncommittedNodeHits;
    extern const Event KeeperLSMTGetUncommittedNodeMisses;
    extern const Event KeeperLSMTGetCommittedNodeFromMemory;
    extern const Event KeeperLSMTGetCommittedNodeNonexistent;
    extern const Event KeeperLSMTGetCommittedNodeLoadedBlock;
    extern const Event KeeperLSMTNodeCacheEntriesUpdated;
    extern const Event KeeperLSMTListNamesFromMemtables;
    extern const Event KeeperLSMTListNamesFromFiles;
    extern const Event KeeperLSMTListScannedBlocks;
    extern const Event KeeperLSMTListScannedEntries;
    extern const Event KeeperLSMTListFilterSkipped;
}

namespace Coordination::Storage
{

StorageState::StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_)
    : keeper_context(std::move(keeper_context_)), log(getLogger("KeeperLSMT")), storage_mutex(storage_mutex_)
{
    const DB::CoordinationSettings & settings = keeper_context->getCoordinationSettings();

    memory_only = settings[DB::CoordinationSetting::storage_memory_only];

    /// pread is the default, but spell it out because positioned reads (readBigAt) on local
    /// disks depend on it.
    read_settings.local_fs_settings.method = DB::LocalFSReadMethod::pread;
    /// Skip the async prefetching wrapper: we don't do sequential reads, only readBigAt.
    read_settings.remote_fs_settings.method = DB::RemoteFSReadMethod::read;

    if (!memory_only)
    {
        size_t block_cache_size = settings[DB::CoordinationSetting::block_cache_size];
        if (block_cache_size == 0)
            block_cache_size = static_cast<size_t>(
                static_cast<double>(getMemoryAmount())
                * static_cast<double>(settings[DB::CoordinationSetting::block_cache_size_ratio]));
        block_cache = std::make_unique<BlockCache>(block_cache_size);
    }
}

StorageState::~StorageState()
{
    shutdown();
}

void StorageState::startup()
{
    if (!memory_only)
    {
        disk = keeper_context->getDataDisk();
        writeInfoFile();
    }

    chassert(!background);
    background = std::make_unique<BackgroundWork>(this);
}

void StorageState::writeInfoFile() const
{
    DB::WriteBufferFromOwnString content_buf;
    DB::writeText("server version: ", content_buf);
    DB::writeText(VERSION_DESCRIBE, content_buf);
    DB::writeText("\nstartup time: ", content_buf);
    const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    DB::writeDateTimeText(DB::DateTime64(now_ns), 9, content_buf);
    DB::writeChar('\n', content_buf);
    const std::string & content = content_buf.str();

    auto out = disk->writeFile("info", DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, write_settings);
    DB::writeString(content, *out);
    out->finalize();

    auto in = disk->readFile("info", read_settings, /*read_hint*/ {});
    if (!in->supportsReadAt())
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Keeper data disk '{}' doesn't support positioned reads, required by the node storage. "
            "Use a 'local' or plain object storage (e.g. 's3_plain') disk.",
            disk->getName());

    std::string read_back(content.size(), '\0');
    const size_t bytes_read = in->readBigAt(read_back.data(), content.size(), 0, /*progress_callback*/ nullptr);
    if (bytes_read != content.size() || read_back != content)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Read unexpected data back from file 'info' on Keeper data disk '{}' ({} bytes instead of {})",
            disk->getName(), bytes_read, content.size());
}

void StorageState::shutdown()
{
    FileDeleteQueuePtr leftover_deletes;
    if (background)
    {
        leftover_deletes = background->file_delete_queue;
        background->shutdown();
        background.reset();
    }

    /// The on-disk node storage is not persistent: it is wiped on startup, and a snapshot install
    /// (`KeeperStateMachine::apply_snapshot`) destroys and recreates the whole storage in-process
    /// without going through that wipe. Remove our files here - otherwise every snapshot install
    /// would leave the previous run set behind on disk until the next full restart.
    if (!memory_only && disk)
    {
        const auto remove_file = [&](const std::string & path)
        {
            try
            {
                disk->removeFileIfExists(path);
            }
            catch (...)
            {
                DB::tryLogCurrentException(log, fmt::format("Failed to delete file {} during shutdown", path));
            }
        };

        for (const auto & run : sorted_runs)
        {
            for (const auto & file : run->files)
            {
                /// ~SortedFile would only re-enqueue the path to the no-longer-drained delete queue.
                file->delete_when_destroyed = false;
                remove_file(file->file_path);
            }
        }

        /// Deletions that were enqueued but not yet performed by the background threads
        /// (including output files of a merge interrupted by the shutdown).
        if (leftover_deletes)
        {
            std::lock_guard lock(leftover_deletes->mutex);
            for (const auto & path : leftover_deletes->paths)
                remove_file(path);
            leftover_deletes->paths.clear();
        }
    }
}

NodeRef StorageState::getCommittedNode(const NodePathWithHash & path) const
{
    const NodeRefCache::Entry * info = nullptr;
    NodeRef node_ref;
    if (node_cache.tryGet(path.hash, node_ref, &info))
    {
        /// Normal fast path: the node is already in memory
        /// (in memtable, or in block cache, or pinned by SortedFile in memory-only mode).
        ProfileEvents::increment(ProfileEvents::KeeperLSMTGetCommittedNodeFromMemory);
        return node_ref;
    }
    if (!info)
    {
        ProfileEvents::increment(ProfileEvents::KeeperLSMTGetCommittedNodeNonexistent);
        return NodeRef{}; // the node is not in NodeCache's map and therefore doesn't exist
    }

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
    size_t entries_updated = 0;
    for (uint32_t offset = block->entries_start; offset < block->size;)
    {
        ref.offset = offset;
        NodePath node_path;
        uint32_t serialized_size = 0;
        NodeAction action = NodeAction::Remove;
        ref.readPath(node_path, path_buf, serialized_size, action);

        if (const auto * node_info = node_cache.findEntry(node_path.calculateHash()))
        {
            /// Don't touch entries whose latest update is in a newer sorted run or memtable.
            if (run.max_file_seqno >= node_info->file_seqno)
            {
                chassert(run.min_file_seqno <= node_info->file_seqno);
                std::lock_guard guard(node_info->block);
                node_info->block.set(block);
                node_info->node_offset = offset;
                entries_updated += 1;
            }
        }

        offset += serialized_size;
    }

    ProfileEvents::increment(ProfileEvents::KeeperLSMTGetCommittedNodeLoadedBlock);
    ProfileEvents::increment(ProfileEvents::KeeperLSMTNodeCacheEntriesUpdated, entries_updated);

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
    {
        if (const auto * lookup = it->nodes.find(path.hash))
        {
            ProfileEvents::increment(ProfileEvents::KeeperLSMTGetUncommittedNodeHits);
            return lookup->getMapped();
        }
    }

    ProfileEvents::increment(ProfileEvents::KeeperLSMTGetUncommittedNodeMisses);

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

            ProfileEvents::increment(ProfileEvents::KeeperLSMTCommittedMemtablesCreated);
            LOG_DEBUG(log, "Creating new memtable {}", mutable_memtable->file_seqno);
        }

        const NodePathHash hash = node.getOrCalculatePathHash();
        auto * node_cache_entry = node_cache.findEntry(hash);
        std::optional<NodeAction> combined;

        /// Validate `action` before mutating anything.
        if (node_cache_entry)
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
        /// (The entry we just appended is the last thing in its block.)
        ProfileEvents::increment(ProfileEvents::KeeperLSMTCommittedEntryBytes, ref.block->size - ref.offset);

        /// Update `node_cache`. (We hold storage_mutex exclusively, so no concurrent readers;
        /// no need for the per-entry spinlocks.)
        if (node_cache_entry)
        {
            if (!combined)
            {
                /// Create + Remove: `node_cache` doesn't keep removed nodes.
                node_cache.eraseEntry(hash, *node_cache_entry);
            }
            else
            {
                node_cache_entry->file_seqno = mutable_memtable->file_seqno;
                node_cache_entry->block.store(ref.block);
                node_cache_entry->node_offset = ref.offset;
            }
        }
        else
        {
            NodeRefCache::Entry & info = node_cache.getOrInsertEntry(hash);
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
        /// Put the exception message into the fatal report: the text log may not survive the
        /// process death, and the crash report is often the only thing test harnesses collect.
        DB::abortOnFailedAssertion(fmt::format(
            "Unexpected exception in StorageState::appendCommittedNode: {}", DB::getCurrentExceptionMessage(/*with_stacktrace=*/ true)));
    }
}

void StorageState::listCommittedChildrenNames(
    const NodePathWithHash & path, ChildrenSet2 & out, DB::Arena & arena) const
{
    /// Visit memtables and sorted runs from newest to oldest, recording the first (newest)
    /// occurrence of each child name in `out`. This includes tombstones; e.g. if the latest
    /// memtable removed a child, its listChildrenNames will insert an action=Remove into `out`,
    /// then listChildrenNames in older memtables and files won't insert this child into `out`.

    const size_t names_before_memtables = out.set.size();

    if (mutable_memtable)
        mutable_memtable->listChildrenNames(path, out, arena);
    for (auto it = immutable_memtables.rbegin(); it != immutable_memtables.rend(); ++it)
        (*it)->listChildrenNames(path, out, arena);

    const size_t names_before_files = out.set.size();
    ProfileEvents::increment(ProfileEvents::KeeperLSMTListNamesFromMemtables, names_before_files - names_before_memtables);

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
            (*it)->listChildrenNames(range_start, range_end, path.hash, out, arena, block_cache.get());
    }

    ProfileEvents::increment(ProfileEvents::KeeperLSMTListNamesFromFiles, out.set.size() - names_before_files);
}

namespace
{

/// One sorted run, positioned on the next direct child in (range_start, range_end).
struct RunChildCursor
{
    const SortedRun * run = nullptr;
    BlockCache * block_cache = nullptr;
    NodePath range_start;
    NodePath range_end;
    UInt128 parent_hash{};

    size_t file_idx = 0;
    size_t block_idx = 0;
    bool block_positioned = false;
    BlockPtr block;
    uint32_t offset = 0;
    std::string path_buf;

    std::string name;
    NodeAction action = NodeAction::Remove;
    bool has_value = false;

    void advance()
    {
        has_value = false;
        if (!run)
            return;

        size_t scanned_blocks = 0;
        size_t scanned_entries = 0;
        SCOPE_EXIT({
            ProfileEvents::increment(ProfileEvents::KeeperLSMTListScannedBlocks, scanned_blocks);
            ProfileEvents::increment(ProfileEvents::KeeperLSMTListScannedEntries, scanned_entries);
        });

        while (file_idx < run->files.size())
        {
            const SortedFile & file = *run->files[file_idx];
            if (file.parent_paths_filter && !file.parent_paths_filter->findHashPair(
                    DB::BloomFilterHashPair{parent_hash.items[0], parent_hash.items[1]}))
            {
                ProfileEvents::increment(ProfileEvents::KeeperLSMTListFilterSkipped);
                ++file_idx;
                block_positioned = false;
                block.reset();
                continue;
            }

            if (!block_positioned)
            {
                if (!file.blocks.empty() && file.blocks.front().min_path.compare(range_end) >= 0)
                    return;
                const auto block_it = std::ranges::partition_point(
                    file.blocks, [&](const SortedFile::BlockInfo & info) { return info.max_path.compare(range_start) <= 0; });
                block_idx = static_cast<size_t>(block_it - file.blocks.begin());
                block_positioned = true;
                block.reset();
            }

            while (block_idx < file.blocks.size())
            {
                if (file.blocks[block_idx].min_path.compare(range_end) >= 0)
                    return;

                if (!block)
                {
                    block = file.getOrLoadBlock(static_cast<uint32_t>(block_idx), block_cache);
                    offset = block->entries_start;
                    scanned_blocks += 1;
                }

                NodeRef ref{.block = block};
                NodePath node_path;
                uint32_t serialized_size = 0;
                NodeAction node_action = NodeAction::Remove;
                while (offset < block->size)
                {
                    ref.offset = offset;
                    ref.readPath(node_path, path_buf, serialized_size, node_action);
                    offset += serialized_size;
                    scanned_entries += 1;

                    if (node_path.compare(range_start) <= 0)
                        continue;
                    if (node_path.compare(range_end) >= 0)
                    {
                        has_value = false;
                        file_idx = run->files.size();
                        return;
                    }

                    name = std::string(node_path.baseName());
                    action = node_action;
                    has_value = true;
                    return;
                }

                ++block_idx;
                block.reset();
            }

            ++file_idx;
            block_positioned = false;
            block.reset();
        }
    }
};

}

void StorageState::listDirectChildrenPage(
    const NodePathWithHash & path,
    std::string_view start_after,
    uint32_t max_results,
    std::vector<std::string> & out,
    bool & truncated) const
{
    out.clear();
    truncated = false;

    /// Newest memtable wins. Children sets are unordered, so this parent’s memtable names are read in full.
    ChildrenSet2 mem_set;
    DB::Arena arena;
    if (mutable_memtable)
        mutable_memtable->listChildrenNames(path, mem_set, arena);
    for (auto it = immutable_memtables.rbegin(); it != immutable_memtables.rend(); ++it)
        (*it)->listChildrenNames(path, mem_set, arena);

    std::vector<std::pair<std::string, NodeAction>> mem_names;
    mem_names.reserve(mem_set.set.size());
    for (const auto & entry : mem_set.set)
    {
        if (entry.str() > start_after)
            mem_names.emplace_back(entry.str(), entry.action);
    }
    std::ranges::sort(mem_names, [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    std::string range_prefix(path.path.str());
    if (!range_prefix.ends_with('/'))
        range_prefix += '/';
    std::string range_start_str = range_prefix;
    range_start_str.append(start_after);
    std::string range_end_str = range_prefix;
    ++range_end_str.back();
    NodePath range_start(range_start_str, path.path.depth + 1);
    const NodePath range_end(range_end_str, path.path.depth + 1);

    std::vector<RunChildCursor> runs;
    runs.reserve(sorted_runs.size());
    for (auto it = sorted_runs.rbegin(); it != sorted_runs.rend(); ++it)
    {
        const SortedRun & run = **it;
        RunChildCursor cursor;
        cursor.run = &run;
        cursor.block_cache = block_cache.get();
        cursor.range_end = range_end;
        cursor.parent_hash = path.hash;
        cursor.range_start = range_start;
        if (run.min_path_cutoff && run.min_path_cutoff->compare(cursor.range_start) > 0)
            cursor.range_start = *run.min_path_cutoff;
        if (cursor.range_start.compare(range_end) >= 0)
            continue;

        cursor.file_idx = static_cast<size_t>(std::partition_point(
            run.files.begin(), run.files.end(),
            [&](const SortedFilePtr & file) { return file->blocks.back().max_path.compare(cursor.range_start) <= 0; })
            - run.files.begin());
        cursor.advance();
        if (cursor.has_value)
            runs.push_back(std::move(cursor));
    }

    struct Head
    {
        std::string name;
        NodeAction action = NodeAction::Remove;
        bool valid = false;
    };
    /// Source 0 is the memtable vector. Sources 1.. are sorted runs, newest first.
    std::vector<Head> heads(runs.size() + 1);
    size_t mem_idx = 0;
    const auto pull = [&](size_t source)
    {
        Head & head = heads[source];
        if (source == 0)
        {
            if (mem_idx >= mem_names.size())
            {
                head.valid = false;
                return;
            }
            head.name = mem_names[mem_idx].first;
            head.action = mem_names[mem_idx].second;
            head.valid = true;
            ++mem_idx;
            return;
        }

        RunChildCursor & cursor = runs[source - 1];
        if (!cursor.has_value)
        {
            head.valid = false;
            return;
        }
        head.name = cursor.name;
        head.action = cursor.action;
        head.valid = true;
        cursor.advance();
    };

    struct HeapItem
    {
        std::string_view name;
        size_t source = 0;
        bool operator>(const HeapItem & rhs) const
        {
            if (name != rhs.name)
                return name > rhs.name;
            return source > rhs.source;
        }
    };
    std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<>> heap;
    for (size_t source = 0; source < heads.size(); ++source)
    {
        pull(source);
        if (heads[source].valid)
            heap.push(HeapItem{.name = heads[source].name, .source = source});
    }

    while (!heap.empty())
    {
        const HeapItem top = heap.top();
        heap.pop();
        /// Copy before `pull`: it overwrites `heads[source].name`, which `top.name` views.
        const std::string name(top.name);
        const NodeAction action = heads[top.source].action;
        pull(top.source);
        if (heads[top.source].valid)
            heap.push(HeapItem{.name = heads[top.source].name, .source = top.source});

        while (!heap.empty() && heap.top().name == name)
        {
            const size_t source = heap.top().source;
            heap.pop();
            pull(source);
            if (heads[source].valid)
                heap.push(HeapItem{.name = heads[source].name, .source = source});
        }

        if (action == NodeAction::Remove)
            continue;

        if (max_results != 0 && out.size() == max_results)
        {
            truncated = true;
            return;
        }
        out.emplace_back(name);
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

void StorageState::fillAsynchronousMetrics(DB::AsynchronousMetricValues & new_values) const
{
    new_values["KeeperLSMTUncommittedMemtablesSize"] = { uncommitted_bytes.load(std::memory_order_relaxed), "Bytes in memtables storing uncommitted state." };

    size_t immutable_memtable_bytes = 0;
    size_t total_entries = 0;
    for (const auto & m : immutable_memtables)
    {
        immutable_memtable_bytes += m->total_bytes;
        total_entries += m->num_entries;
    }
    if (mutable_memtable)
        total_entries += mutable_memtable->num_entries;

    new_values["KeeperLSMTImmutableMemtablesCount"] = { immutable_memtables.size(), "Number of memtables waiting for flush." };
    new_values["KeeperLSMTImmutableMemtablesSize"] = { immutable_memtable_bytes, "Bytes in memtables waiting for flush." };
    new_values["KeeperLSMTMutableMemtableSize"] = { mutable_memtable ? mutable_memtable->total_bytes : 0, "Bytes in current memtable." };

    new_values["KeeperLSMTNodeCacheEntries"] = { node_cache.size(), "Number of znodes in node cache." };
    new_values["KeeperLSMTThrottling"] = { write_throttling_us.load(), "Microseconds of delay added to each write because background flushes or merges fell behind. 0 if background work is keeping up." };
    new_values["KeeperLSMTMergesInProgress"] = { background ? background->merges_running.load() : 0, "Number of background merges running." };

    size_t total_files = 0;
    size_t files_compressed_bytes = 0;
    size_t files_uncompressed_bytes = 0;
    size_t parent_paths_filter_bytes = 0;
    for (const auto & r : sorted_runs)
    {
        total_files += r->files.size();
        total_entries += r->total_entries;
        files_compressed_bytes += r->total_file_size;
        files_uncompressed_bytes += r->total_block_size;
        for (const auto & f : r->files)
            if (f->parent_paths_filter)
                parent_paths_filter_bytes += f->parent_paths_filter->getFilterSizeBytes();
    }

    new_values["KeeperLSMTSortedRuns"] = { sorted_runs.size(), "Number of sorted runs." };
    new_values["KeeperLSMTFilesCount"] = { total_files, "Number of files in all sorted runs." };
    new_values["KeeperLSMTFilesCompressedSize"] = { files_compressed_bytes, "Total size of files in all sorted runs." };
    new_values["KeeperLSMTFilesUncompressedSize"] = { files_uncompressed_bytes, "Total size of uncompressed blocks in all sorted runs." };
    new_values["KeeperLSMTParentPathsFilterSize"] = { parent_paths_filter_bytes, "Total size of bloom filters of nodes with children, across all files." };

    new_values["KeeperLSMTTotalEntries"] = { total_entries, "Number of entries (nodes and tombstones) in all committed memtables and files." };
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

            ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedMemtablesCreated);
        }

        UncommittedMemtable & u = uncommitted.back();
        u.max_zxid = std::max(u.max_zxid, zxid);
        const size_t bytes_before = u.memtable->total_bytes;
        /// strict=false: see the comment at Memtable::appendNode.
        NodeRef ref = u.memtable->appendNode(node, /*strict=*/ false);
        uncommitted_bytes.fetch_add(u.memtable->total_bytes - bytes_before, std::memory_order_relaxed);

        /// (The entry we just appended is the last thing in its block.)
        const size_t entry_bytes = ref.block->size - ref.offset;
        switch (node.action)
        {
            case NodeAction::Create:
                ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedCreates);
                ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedCreateBytes, entry_bytes);
                break;
            case NodeAction::Update:
                ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedUpdates);
                ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedUpdateBytes, entry_bytes);
                break;
            case NodeAction::Remove:
                ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedRemoves);
                ProfileEvents::increment(ProfileEvents::KeeperLSMTUncommittedRemoveBytes, entry_bytes);
                break;
        }
        /// Loose model: the last record for a path wins, including Remove tombstones.
        u.nodes[node.getOrCalculatePathHash()] = ref;
        return ref;
    }
    catch (...)
    {
        /// Maybe MEMORY_LIMIT_EXCEEDED is possible here. We currently don't handle it, and the
        /// caller doesn't handle it.
        /// Put the exception message into the fatal report: the text log may not survive the
        /// process death, and the crash report is often the only thing test harnesses collect.
        DB::abortOnFailedAssertion(fmt::format(
            "Unexpected exception in StorageState::appendUncommittedNode: {}", DB::getCurrentExceptionMessage(/*with_stacktrace=*/ true)));
    }
}

void StorageState::cleanupUncommittedState(int64_t committed_zxid)
{
    while (!uncommitted.empty() && uncommitted.front().max_zxid <= committed_zxid)
    {
        LOG_DEBUG(log, "Removing obsolete uncommitted memtable with max_zxid = {} (committed_zxid = {})", uncommitted.front().max_zxid, committed_zxid);

        uncommitted_bytes.fetch_sub(uncommitted.front().memtable->total_bytes, std::memory_order_relaxed);
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
    {
        ProfileEvents::increment(ProfileEvents::KeeperLSMTThrottledWrites);
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }
}

std::string StorageState::makeSortedFilePath(uint32_t min_file_seqno, uint32_t max_file_seqno, size_t file_idx_in_run) const
{
    return fmt::format("{}_{}_{}_{:016x}", min_file_seqno, max_file_seqno, file_idx_in_run, thread_local_rng());
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
