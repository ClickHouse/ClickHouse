#include "MergeTreeDataPartInMemory.h"
#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterInMemory.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
       MergeTreeData & storage_,
        const String & name_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, disk_, relative_path_, Type::IN_MEMORY)
{
}

MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DiskPtr & disk_,
        const std::optional<String> & relative_path_)
    : IMergeTreeDataPart(storage_, name_, info_, disk_, relative_path_, Type::IN_MEMORY)
{
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartInMemory::getReader(
    const NamesAndTypesList & columns_to_read,
    const MarkRanges & mark_ranges,
    UncompressedCache * /* uncompressed_cache */,
    MarkCache * /* mark_cache */,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & /* avg_value_size_hints */,
    const ReadBufferFromFileBase::ProfileCallback & /* profile_callback */) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartInMemory>(shared_from_this());
    return std::make_unique<MergeTreeReaderInMemory>(
        ptr, columns_to_read, mark_ranges, reader_settings);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartInMemory::getWriter(
    const NamesAndTypesList & columns_list,
    const std::vector<MergeTreeIndexPtr> & /* indices_to_recalc */,
    const CompressionCodecPtr & /* default_codec */,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & /* computed_index_granularity */) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartInMemory>(shared_from_this());
    return std::make_unique<MergeTreeDataPartWriterInMemory>(ptr, columns_list, writer_settings);
}

void MergeTreeDataPartInMemory::makeCloneInDetached(const String & prefix) const
{
    String detached_path = getRelativePathForDetachedPart(prefix);
    String destination_path = storage.getRelativeDataPath() + getRelativePathForDetachedPart(prefix);

    auto new_type = storage.choosePartTypeOnDisk(block.bytes(), rows_count);
    auto new_data_part = storage.createPart(name, new_type, info, disk, detached_path);

    new_data_part->setColumns(columns);
    new_data_part->partition.value.assign(partition.value);
    new_data_part->minmax_idx = minmax_idx;

    if (disk->exists(destination_path))
    {
        LOG_WARNING(&Logger::get(storage.getLogName()), "Removing old temporary directory " + disk->getPath() + destination_path);
        disk->removeRecursive(destination_path);
    }

    disk->createDirectories(destination_path);

    auto compression_codec = storage.global_context.chooseCompressionCodec(0, 0);
    MergedBlockOutputStream out(new_data_part, columns, storage.skip_indices, compression_codec);
    out.writePrefix();
    out.write(block);
    out.writeSuffixAndFinalizePart(new_data_part);

    if (storage.getSettings()->in_memory_parts_enable_wal)
        storage.getWriteAheadLog()->dropPart(name);
}

bool MergeTreeDataPartInMemory::waitUntilMerged(size_t timeout) const
{
    auto lock = storage.lockParts();
    return is_merged.wait_for(lock, std::chrono::milliseconds(timeout),
        [this]() { return state == State::Outdated; });
}

void MergeTreeDataPartInMemory::notifyMerged() const
{
    is_merged.notify_one();
}

void MergeTreeDataPartInMemory::calculateEachColumnSizesOnDisk(ColumnSizeByName & /*each_columns_size*/, ColumnSize & /*total_size*/) const
{
    // throw Exception("calculateEachColumnSizesOnDisk of in memory part", ErrorCodes::NOT_IMPLEMENTED);
}

void MergeTreeDataPartInMemory::loadIndexGranularity()
{
    throw Exception("loadIndexGranularity of in memory part", ErrorCodes::NOT_IMPLEMENTED);
}

}
