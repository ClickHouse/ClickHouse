#include "MergeTreeDataPartInMemory.h"
#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterInMemory.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
}


MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
       MergeTreeData & storage_,
        const String & name_,
        const DataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, data_part_storage_, Type::InMemory, parent_part_)
{
    default_codec = CompressionCodecFactory::instance().get("NONE", {});
}

MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const DataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, data_part_storage_, Type::InMemory, parent_part_)
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
    DataPartStorageBuilderPtr data_part_storage_builder_,
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & /* indices_to_recalc */,
    const CompressionCodecPtr & /* default_codec */,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & /* computed_index_granularity */) const
{
    data_part_storage_builder = data_part_storage_builder_;
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartInMemory>(shared_from_this());
    return std::make_unique<MergeTreeDataPartWriterInMemory>(
        ptr, columns_list, metadata_snapshot, writer_settings);
}

DataPartStoragePtr MergeTreeDataPartInMemory::flushToDisk(const String & new_relative_path, const StorageMetadataPtr & metadata_snapshot) const
{
    auto current_full_path = data_part_storage_builder->getFullPath();
    data_part_storage_builder->setRelativePath(new_relative_path);

    auto new_type = storage.choosePartTypeOnDisk(block.bytes(), rows_count);
    auto new_data_part_storage = data_part_storage_builder->getStorage();
    auto new_data_part = storage.createPart(name, new_type, info, new_data_part_storage);

    new_data_part->uuid = uuid;
    new_data_part->setColumns(columns, {});
    new_data_part->partition.value = partition.value;
    new_data_part->minmax_idx = minmax_idx;

    if (data_part_storage_builder->exists())
    {
        throw Exception(
            ErrorCodes::DIRECTORY_ALREADY_EXISTS,
            "Could not flush part {}. Part in {} already exists",
            quoteString(current_full_path),
            data_part_storage_builder->getFullPath());
    }

    data_part_storage_builder->createDirectories();

    auto compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);
    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());
    MergedBlockOutputStream out(new_data_part, data_part_storage_builder, metadata_snapshot, columns, indices, compression_codec, NO_TRANSACTION_PTR);
    out.write(block);
    const auto & projections = metadata_snapshot->getProjections();
    for (const auto & [projection_name, projection] : projection_parts)
    {
        if (projections.has(projection_name))
        {
            auto projection_part_storage_builder = data_part_storage_builder->getProjection(projection_name + ".proj");
            if (projection_part_storage_builder->exists())
            {
                throw Exception(
                    ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                    "Could not flush projection part {}. Projection part in {} already exists",
                    projection_name,
                    projection_part_storage_builder->getFullPath());
            }

            auto projection_part = asInMemoryPart(projection);
            auto projection_type = storage.choosePartTypeOnDisk(projection_part->block.bytes(), rows_count);
            MergeTreePartInfo projection_info("all", 0, 0, 0);
            auto projection_data_part
                = storage.createPart(projection_name, projection_type, projection_info, projection_part_storage_builder->getStorage(), parent_part);
            projection_data_part->is_temp = false; // clean up will be done on parent part
            projection_data_part->setColumns(projection->getColumns(), {});

            projection_part_storage_builder->createDirectories();
            const auto & desc = projections.get(name);
            auto projection_compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);
            auto projection_indices = MergeTreeIndexFactory::instance().getMany(desc.metadata->getSecondaryIndices());
            MergedBlockOutputStream projection_out(
                projection_data_part, projection_part_storage_builder, desc.metadata, projection_part->columns, projection_indices,
                projection_compression_codec, NO_TRANSACTION_PTR);

            projection_out.write(projection_part->block);
            projection_out.finalizePart(projection_data_part, false);
            new_data_part->addProjectionPart(projection_name, std::move(projection_data_part));
        }
    }

    out.finalizePart(new_data_part, false);
    return new_data_part_storage;
}

void MergeTreeDataPartInMemory::makeCloneInDetached(const String & prefix, const StorageMetadataPtr & metadata_snapshot) const
{
    String detached_path = getRelativePathForDetachedPart(prefix);
    flushToDisk(detached_path, metadata_snapshot);
}

void MergeTreeDataPartInMemory::renameTo(const String & new_relative_path, bool /* remove_new_dir_if_exists */, DataPartStorageBuilderPtr) const
{
    data_part_storage->setRelativePath(new_relative_path);

    if (data_part_storage_builder)
        data_part_storage_builder->setRelativePath(new_relative_path);
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
