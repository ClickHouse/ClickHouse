#include "MergeTreeDataPartInMemory.h"
#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterInMemory.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
}


MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
       MergeTreeData & storage_,
        const String & name_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, volume_, relative_path_, Type::IN_MEMORY, parent_part_)
{
    default_codec = CompressionCodecFactory::instance().get("NONE", {});
}

MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const VolumePtr & volume_,
        const std::optional<String> & relative_path_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, volume_, relative_path_, Type::IN_MEMORY, parent_part_)
{
    default_codec = CompressionCodecFactory::instance().get("NONE", {});
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartInMemory::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * /* uncompressed_cache */,
    MarkCache * /* mark_cache */,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & /* avg_value_size_hints */,
    const ReadBufferFromFileBase::ProfileCallback & /* profile_callback */) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartInMemory>(shared_from_this());
    return std::make_unique<MergeTreeReaderInMemory>(
        ptr, columns_to_read, metadata_snapshot, mark_ranges, reader_settings);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartInMemory::getWriter(
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & /* indices_to_recalc */,
    const CompressionCodecPtr & /* default_codec */,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & /* computed_index_granularity */) const
{
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartInMemory>(shared_from_this());
    return std::make_unique<MergeTreeDataPartWriterInMemory>(
        ptr, columns_list, metadata_snapshot, writer_settings);
}

void MergeTreeDataPartInMemory::flushToDisk(const String & base_path, const String & new_relative_path, const StorageMetadataPtr & metadata_snapshot) const
{
    const auto & disk = volume->getDisk();
    String destination_path = base_path + new_relative_path;

    auto new_type = storage.choosePartTypeOnDisk(block.bytes(), rows_count);
    auto new_data_part = storage.createPart(name, new_type, info, volume, new_relative_path);

    new_data_part->uuid = uuid;
    new_data_part->setColumns(columns);
    new_data_part->partition.value = partition.value;
    new_data_part->minmax_idx = minmax_idx;

    if (disk->exists(destination_path))
    {
        throw Exception("Could not flush part " + quoteString(getFullPath())
            + ". Part in " + fullPath(disk, destination_path) + " already exists", ErrorCodes::DIRECTORY_ALREADY_EXISTS);
    }

    disk->createDirectories(destination_path);

    auto compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);
    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns, indices, compression_codec);
    out.writePrefix();
    out.write(block);
    out.writeSuffixAndFinalizePart(new_data_part);
}

void MergeTreeDataPartInMemory::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const
{
    String detached_path = getRelativePathForDetachedPart(prefix);
    flushToDisk(storage.getRelativeDataPath(), detached_path, metadata_snapshot);
}

void MergeTreeDataPartInMemory::renameTo(const String & new_relative_path, bool /* remove_new_dir_if_exists */) const
{
    relative_path = new_relative_path;
}

void MergeTreeDataPartInMemory::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const
{
    auto it = checksums.files.find("data.bin");
    if (it != checksums.files.end())
        total_size.data_uncompressed += it->second.uncompressed_size;

    for (const auto & column : columns)
        each_columns_size[column.name].data_uncompressed += block.getByName(column.name).column->byteSize();
}

IMergeTreeDataPart::Checksum MergeTreeDataPartInMemory::calculateBlockChecksum() const
{
    SipHash hash;
    IMergeTreeDataPart::Checksum checksum;
    for (const auto & column : block)
        column.column->updateHashFast(hash);

    checksum.uncompressed_size = block.bytes();
    hash.get128(checksum.uncompressed_hash);
    return checksum;
}

DataPartInMemoryPtr asInMemoryPart(const MergeTreeDataPartPtr & part)
{
    return std::dynamic_pointer_cast<const MergeTreeDataPartInMemory>(part);
}
}
