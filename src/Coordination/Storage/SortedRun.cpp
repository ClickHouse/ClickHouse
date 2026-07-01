#include <Coordination/Storage/SortedRun.h>

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/Storage/SortedFile.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <base/defines.h>

#include <algorithm>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 file_block_size;
    extern const CoordinationSettingsUInt64 sorted_file_uncompressed_size;
}

namespace Coordination::Storage
{

SortedRun::SortedRun(uint32_t min_file_seqno_, uint32_t max_file_seqno_)
    : min_file_seqno(min_file_seqno_)
    , max_file_seqno(max_file_seqno_)
{
}

SortedRunPtr SortedRun::shallowCopy() const
{
    SortedRunPtr copy(new SortedRun(*this));
    copy->setMinPathCutoff(min_path_cutoff);
    return copy;
}

void SortedRun::setMinPathCutoff(std::optional<NodePath> new_cutoff)
{
    min_path_cutoff = new_cutoff;
    if (min_path_cutoff)
    {
        min_path_buf = min_path_cutoff->str();
        min_path_cutoff->ptr = min_path_buf.data();
    }
}

BlockPtr SortedRun::getBlockCoveringPath(NodePath path, BlockCache * block_cache) const
{
    if (min_path_cutoff && path.compare(*min_path_cutoff) <= 0)
        return {};

    auto file_it = std::partition_point(
        files.begin(), files.end(),
        [&](const SortedFilePtr & f) { return f->blocks.back().max_path.compare(path) < 0; });
    if (file_it == files.end())
        return {};
    return (*file_it)->getBlockCoveringPath(path, block_cache);
}

void SortedRun::listChildrenNames(
    NodePath range_start, NodePath range_end, ChildrenSet2 & out, DB::Arena & arena, BlockCache * block_cache) const
{
    /// Tighten the (exclusive) lower bound by the run's cutoff: nodes <= cutoff were merged away.
    if (min_path_cutoff && min_path_cutoff->compare(range_start) > 0)
        range_start = *min_path_cutoff;
    if (range_start.compare(range_end) >= 0)
        return; // empty range (cutoff is past all children)

    /// The range may span several files. Find the first file that may contain a node > range_start,
    /// and iterate until a file starts at/after range_end.
    auto file_it = std::partition_point(
        files.begin(), files.end(),
        [&](const SortedFilePtr & f) { return f->blocks.back().max_path.compare(range_start) <= 0; });
    for (; file_it != files.end(); ++file_it)
    {
        const SortedFile & file = **file_it;
        if (file.blocks.front().min_path.compare(range_end) >= 0)
            break;
        file.listChildrenNames(range_start, range_end, out, arena, block_cache);
    }
}

SortedRunWriter::SortedRunWriter(SortedRunPtr sorted_run_, StorageState * storage_)
    : storage(storage_)
    , target_block_size(storage_->keeper_context->getCoordinationSettings()[DB::CoordinationSetting::file_block_size])
    , target_file_uncompressed_size(storage_->keeper_context->getCoordinationSettings()[DB::CoordinationSetting::sorted_file_uncompressed_size])
    , sorted_run(std::move(sorted_run_))
{
}

SortedRunWriter::~SortedRunWriter()
{
    if (!storage->memory_only && sorted_run)
    {
        /// TODO: Delete files, catch+log+ignore exceptions.
    }
}

void SortedRunWriter::appendNode(FullNode & node)
{
    if (!file)
    {
        file = std::make_shared<SortedFile>();
        file->serialization_version = SERIALIZATION_VERSION_LATEST;
        file->min_compatible_version = SERIALIZATION_VERSION_LATEST;
        file->digest_version = DB::KEEPER_CURRENT_DIGEST_VERSION;

        if (!storage->memory_only)
        {
            /// TODO: Assign file->file_path.
        }
    }

    file->node_count_delta += nodeCountDelta(node.action);

    BlockPtr new_block;
    NodeRef ref;
    if (BlockData::appendNodeOrStartNewBlock(block, node, target_block_size, new_block, ref))
    {
        finishBlock();

        block = new_block;
        block_min_path = node.path;
        block_min_path.ptr = file->arena.insert(block_min_path.ptr, block_min_path.len);
    }

    block_max_path = node.path;
    block_max_path_buf = block_max_path.str();
    block_max_path.ptr = block_max_path_buf.data();
}

bool SortedRunWriter::finishFileIfBigEnough()
{
    if (file && file->total_block_size + block->size >= target_file_uncompressed_size)
    {
        finishFile();
        return true;
    }
    return false;
}

void SortedRunWriter::finishBlock()
{
    if (!block)
        return;

    uint32_t block_idx = static_cast<uint32_t>(file->blocks.size());
    auto & info = file->blocks.emplace_back();
    info.min_path = block_min_path;
    block_max_path.ptr = file->arena.insert(block_max_path.ptr, block_max_path.len);
    info.max_path = block_max_path;
    info.data.store(block);

    file->total_block_size += block->size;

    if (storage->memory_only)
    {
        file->pinned_blocks.push_back(block);
        file->file_size += block->capacity;
    }
    else
    {
        chassert(storage->block_cache);
        storage->block_cache->insertProbationary(BlockCacheKey {.file_id = file->file_id, .block_idx = block_idx}, block);

        /// TODO: Add `block` to block group, finishGroup() if group got big enough.
    }

    block.reset();
}

void SortedRunWriter::finishFile()
{
    if (!file)
        return;

    /// Note: this is required because finishFileIfBigEnough() must make all appended nodes visible
    /// in sorted_run when returning true. Merge relies on this when publishing partial results.
    finishBlock();

    /// TODO: Write file footer, close file.

    chassert(!file->blocks.empty()); // a file is created only when a node is appended
    sorted_run->total_block_size += file->total_block_size;
    sorted_run->total_file_size += file->file_size;
    sorted_run->files.push_back(std::move(file));
}

SortedRunPtr SortedRunWriter::finish()
{
    finishFile();
    return std::move(sorted_run);
}

}
