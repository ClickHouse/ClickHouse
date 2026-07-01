#include <Coordination/Storage/NodeStream.h>

#include <Coordination/Storage/SortedFile.h>
#include <Coordination/Storage/StorageState.h>
#include <base/defines.h>

#include <algorithm>

namespace Coordination::Storage
{

SortedRunNodeStream::SortedRunNodeStream(SortedRunPtr sorted_run_, BlockCache * block_cache_)
    : sorted_run(std::move(sorted_run_)), block_cache(block_cache_)
{
}

void SortedRunNodeStream::next()
{
    chassert(!at_end);

    for (;;)
    {
        if (!block)
        {
            if (file_idx >= sorted_run->files.size())
            {
                at_end = true;
                return;
            }
            const SortedFile & file = *sorted_run->files[file_idx];
            if (next_block_idx >= file.blocks.size())
            {
                ++file_idx;
                next_block_idx = 0;
                continue;
            }
            const SortedFile::BlockInfo & block_info = file.blocks[next_block_idx];
            ++next_block_idx;
            if (sorted_run->min_path_cutoff && block_info.max_path.compare(*sorted_run->min_path_cutoff) <= 0)
                /// Quickly skip blocks entirely at/below the run's cutoff.
                continue;
            block = file.getOrLoadBlock(next_block_idx - 1, block_cache);
            offset = block->entries_start;
        }

        if (offset >= block->size)
        {
            block.reset();
            continue;
        }

        NodeRef ref {.action = NodeAction::Remove, .offset = offset, .block = block};
        ref.read(node, path_buf);
        offset += node.serialized_size;

        /// Skip nodes at/below the run's cutoff (they were merged into another run).
        if (sorted_run->min_path_cutoff && node.path.compare(*sorted_run->min_path_cutoff) <= 0)
            continue;

        return;
    }
}

void SortedRunNodeStream::dropConsumedFiles(std::vector<SortedFilePtr> & dropped)
{
    for (size_t i = 0; i < file_idx; ++i)
    {
        SortedFilePtr file = std::move(sorted_run->files[i]);
        sorted_run->total_block_size -= file->total_block_size;
        sorted_run->total_file_size -= file->file_size;
        dropped.push_back(std::move(file));
    }
    sorted_run->files.erase(sorted_run->files.begin(), sorted_run->files.begin() + file_idx);
    file_idx = 0;
}

bool MemtableSortedNodeStream::NodeInfo::operator<(const NodeInfo & rhs) const
{
    int cmp = path.compare(rhs.path);
    if (cmp != 0)
        return cmp < 0;
    return idx < rhs.idx;
}

MemtableSortedNodeStream::MemtableSortedNodeStream(MemtablePtr memtable_)
    : memtable(std::move(memtable_))
{
    std::string path_buf;
    uint32_t idx = 0;
    for (uint32_t block_idx = 0; block_idx < memtable->blocks.size(); ++block_idx)
    {
        const BlockPtr & block = memtable->blocks[block_idx];
        NodeRef ref {.action = NodeAction::Remove, .offset = 0, .block = block};
        for (uint32_t offset = block->entries_start; offset < block->size;)
        {
            ref.offset = offset;
            NodePath path;
            uint32_t serialized_size = 0;
            NodeAction action = NodeAction::Remove;
            ref.readPath(path, path_buf, serialized_size, action);
            path.ptr = arena.insert(path.ptr, path.len);

            sorted_nodes.push_back(NodeInfo {.path = path, .block_idx = block_idx, .offset = offset, .idx = idx});
            ++idx;
            offset += serialized_size;
        }
    }

    std::sort(sorted_nodes.begin(), sorted_nodes.end());
}

void MemtableSortedNodeStream::next()
{
    chassert(!at_end);
    if (next_idx == sorted_nodes.size())
    {
        at_end = true;
        return;
    }

    const NodeInfo & info = sorted_nodes[next_idx];
    ++next_idx;

    NodeRef ref {.action = NodeAction::Create, .offset = info.offset, .block = memtable->blocks[info.block_idx]};
    ref.readWithKnownPath(node, info.path);
}

bool MemtableSortedNodeStream::isNextPathEqualToCurrent() const
{
    chassert(!at_end && next_idx > 0);
    if (next_idx == sorted_nodes.size())
        return false;
    return node.path.compare(sorted_nodes[next_idx].path) == 0;
}

MemtableReversedNodeStream::MemtableReversedNodeStream(MemtablePtr memtable_)
    : memtable(std::move(memtable_))
{
    block_idx = static_cast<uint32_t>(memtable->blocks.size());
}

void MemtableReversedNodeStream::next()
{
    chassert(!at_end);

    while (idx == 0) // current block exhausted
    {
        if (block_idx == 0)
        {
            at_end = true;
            return;
        }
        --block_idx;

        const BlockPtr & block = memtable->blocks[block_idx];
        offsets.clear();
        NodeRef ref {.action = NodeAction::Remove, .offset = 0, .block = block};
        for (uint32_t offset = block->entries_start; offset < block->size;)
        {
            ref.offset = offset;
            offsets.push_back(offset);
            offset += ref.readSerializedSize();
        }
        idx = offsets.size();
    }

    --idx;
    NodeRef ref {.action = NodeAction::Remove, .offset = offsets[idx], .block = memtable->blocks[block_idx]};
    ref.read(node, path_buf);
}

bool MergingNodeStream::Substream::operator<(const Substream & rhs) const
{
    int cmp = stream->node.path.compare(rhs.stream->node.path);
    if (cmp != 0)
        /// Reversed because that's how std::priority_queue likes it.
        return cmp > 0;
    return idx > rhs.idx;
}

void MergingNodeStream::advanceAndPush(Substream s)
{
    s.stream->next();
    if (!s.stream->at_end)
        pq.push(s);
}

void MergingNodeStream::addSubstream(NodeStream * s)
{
    advanceAndPush({s, next_idx});
    ++next_idx;
}

void MergingNodeStream::next()
{
    chassert(!at_end);

    if (pinned_substream.has_value())
    {
        advanceAndPush(*pinned_substream);
        pinned_substream.reset();
    }

    while (true) // one path per iteration
    {
        if (pq.empty())
        {
            at_end = true;
            return;
        }

        Substream substream = pq.top();
        pq.pop();

        /// Invariant: {combined_action, substream.stream->node} is the combined node+action for the
        /// current path so far.
        std::optional<NodeAction> combined_action = substream.stream->node.action;
        while (!pq.empty() && pq.top().stream->node.path.compare(substream.stream->node.path) == 0)
        {
            Substream next_substream = pq.top();
            pq.pop();

            chassert(next_substream.idx > substream.idx); // fails if a substream has duplicate paths

            /// `substream` and `next_substream` have two consecutive nodes with equal path.
            combined_action = combineActions(combined_action, next_substream.stream->node.action, /*strict=*/ true);

            advanceAndPush(substream);
            substream = next_substream;
        }

        if (combined_action.has_value())
        {
            node = substream.stream->node;
            node.action = *combined_action;
            /// Defer advancing `substream` until the next `next()` call, because we're borrowing
            /// its `node`.
            pinned_substream = substream;
            return;
        }

        /// Nodes cancelled out, go to next path.
        advanceAndPush(substream);
    }
}

SnapshotWriterNodeStream::SnapshotWriterNodeStream(const StorageState & storage)
{
    block_cache = storage.block_cache.get();
    sorted_runs = storage.sorted_runs;
    memtables = storage.immutable_memtables;

    if (storage.mutable_memtable)
        memtables.push_back(storage.mutable_memtable->takeSnapshot());
}

size_t SnapshotWriterNodeStream::getNodeCount() const
{
    int64_t sum = 0;
    for (const auto & r : sorted_runs)
        sum += r->node_count_delta;
    for (const auto & m : memtables)
        sum += m->node_count_delta;
    chassert(sum >= 0);
    return size_t(sum);
}

void SnapshotWriterNodeStream::next()
{
    chassert(!at_end);
    while (true)
    {
        /// Read from memtables.

        if (memtable_stream)
        {
            memtable_stream->next();
            if (memtable_stream->at_end)
            {
                memtable_stream.reset();
                continue;
            }

            NodePathHash path_hash = memtable_stream->node.getOrCalculatePathHash();
            auto [_, inserted] = memtable_paths.insert(path_hash);
            if (!inserted)
                continue;

            /// Don't emit tombstone, but it's important that we added it to `memtable_paths`.
            if (memtable_stream->node.action == NodeAction::Remove)
                continue;

            node = memtable_stream->node;
            node.action = NodeAction::Create;
            return;
        }

        if (!memtables.empty())
        {
            memtable_stream.emplace(memtables.back());
            memtables.pop_back();
            continue;
        }

        /// Read from files.

        if (!merging_stream)
        {
            chassert(run_streams.empty());
            run_streams.reserve(sorted_runs.size());
            for (const auto & r : sorted_runs)
                run_streams.emplace_back(r, block_cache);
            merging_stream.emplace();
            for (auto & s : run_streams)
                merging_stream->addSubstream(&s);
        }

        merging_stream->next();
        if (merging_stream->at_end)
        {
            at_end = true;
            return;
        }
        chassert(merging_stream->node.action == NodeAction::Create);

        {
            NodePathHash path_hash = merging_stream->node.getOrCalculatePathHash();
            if (memtable_paths.contains(path_hash))
                continue;
        }

        node = merging_stream->node;
        return;
    }
}

}
