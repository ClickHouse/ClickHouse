#include <Coordination/Storage/SortedRun.h>

#include <Coordination/Storage/BackgroundWork.h>
#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/Storage/SortedFile.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>
#include <Core/Defines.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/CompressionMethod.h>
#include <base/defines.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 file_block_size;
    extern const CoordinationSettingsUInt64 sorted_file_uncompressed_size;
    extern const CoordinationSettingsUInt64 file_block_group_compressed_size;
}

namespace Coordination::Storage
{

AppendWriteBuffer::AppendWriteBuffer(WriteBuffer * out_) : WriteBuffer(out_->position(), out_->available()), out(out_) {}

void AppendWriteBuffer::nextImpl()
{
    flush();
    out->next();
    set(out->position(), out->available());
}

void AppendWriteBuffer::finalizeImpl()
{
    flush();
}

AppendWriteBuffer::~AppendWriteBuffer() = default;

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
    : sorted_run(std::move(sorted_run_)), storage(storage_)
{
    const auto & settings = storage_->keeper_context->getCoordinationSettings();
    target_block_size = settings[DB::CoordinationSetting::file_block_size];
    target_block_group_compressed_size = settings[DB::CoordinationSetting::file_block_group_compressed_size];
    target_file_uncompressed_size = settings[DB::CoordinationSetting::sorted_file_uncompressed_size];
}

SortedRunWriter::~SortedRunWriter()
{
    if (compressed_writer)
    {
        compressed_writer->cancel();
        compressed_writer.reset();
    }

    if (file_writer)
    {
        file_writer->cancel();
        file_writer.reset();
    }

    /// (No need to explicitly delete incomplete files on exception: unpublished files still have
    ///  delete_when_destroyed == true and enqueue their own deletion in ~SortedFile.)
}

void SortedRunWriter::appendNode(FullNode & node)
{
    if (file)
        /// Assert the input is sorted.
        chassert(node.path.compare(block_max_path) > 0);

    if (!file)
    {
        file = std::make_shared<SortedFile>();
        file->serialization_version = SERIALIZATION_VERSION_LATEST;
        file->min_compatible_version = SERIALIZATION_VERSION_LATEST;
        file->digest_version = DB::KEEPER_CURRENT_DIGEST_VERSION;

        if (!storage->memory_only)
        {
            file->file_path = storage->makeSortedFilePath(
                sorted_run->min_file_seqno, sorted_run->max_file_seqno, sorted_run->files.size());
            /// If the flush/merge fails or is cancelled, the file deletes itself.
            /// The publisher flips this to false.
            file->delete_when_destroyed = true;
            file->file_deleter = storage->background->file_delete_queue;
            file_writer = storage->disk->writeFile(
                file->file_path, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, storage->write_settings);
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
    info.block_size = block->size;
    info.data.store(block);

    file->total_block_size += info.block_size;

    if (storage->memory_only)
    {
        file->pinned_blocks.push_back(block);
        file->file_size += block->capacity;
    }
    else
    {
        chassert(storage->block_cache);
        storage->block_cache->insertProbationary(BlockCacheKey {.file_id = file->file_id, .block_idx = block_idx}, block);

        if (!compressed_writer)
        {
            group_start_block_idx = block_idx;
            group_offset_in_file = file_writer->count();

            /// We want to target a compressed rather than uncompressed group size, to control size
            /// of file reads (e.g. for S3 we want to read in chunks of a few hundred KB).
            /// So here we make sure we flush data to the compressor (which writes it to output file)
            /// frequently enough to not go very far over the threshold.
            size_t buf_size = std::min(size_t(DB::DBMS_DEFAULT_BUFFER_SIZE), target_block_group_compressed_size / 2);

            file_appender.emplace(file_writer.get());
            compressed_writer = DB::wrapWriteBufferWithCompressionMethod(
                &*file_appender, DB::CompressionMethod::Zstd,
                /*level=*/ 3, /*zstd_window_log=*/ 0,
                buf_size);
        }

        info.offset_in_group = compressed_writer->count();

        compressed_writer->write(block->data(), block->size);

        file_appender->flush();
        if (file_writer->count() - group_offset_in_file > target_block_group_compressed_size)
            finishGroup();
    }

    block.reset();
}

void SortedRunWriter::finishGroup()
{
    if (!compressed_writer)
        return;

    compressed_writer->finalize();
    compressed_writer.reset();
    file_appender.reset();

    size_t group_compressed_size = file_writer->count() - group_offset_in_file;
    chassert(group_compressed_size > 0);

    for (size_t block_idx = group_start_block_idx; block_idx < file->blocks.size(); ++block_idx)
    {
        auto & info = file->blocks[block_idx];
        info.group_offset_in_file = group_offset_in_file;
        info.group_compressed_size = group_compressed_size;
    }
}

void SortedRunWriter::finishFile()
{
    if (!file)
        return;

    finishBlock();
    finishGroup();

    if (!storage->memory_only)
    {
        /// TODO: Write file footer, if we want files to be usable after restart.

        file->file_size = file_writer->count();

        file_writer->finalize();
        file_writer.reset();

        file->prepareReadBuffer(storage);
    }

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
