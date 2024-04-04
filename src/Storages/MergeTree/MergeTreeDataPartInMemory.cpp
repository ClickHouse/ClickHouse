#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterInMemory.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <DataTypes/NestedUtils.h>
#include <Disks/createVolume.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeDataPartInMemory::MergeTreeDataPartInMemory(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_)
    : IMergeTreeDataPart(storage_, name_, info_, data_part_storage_, Type::InMemory, parent_part_)
{
    default_codec = CompressionCodecFactory::instance().get("NONE", {});
}

IMergeTreeDataPart::MergeTreeReaderPtr MergeTreeDataPartInMemory::getReader(
    const NamesAndTypesList & columns_to_read,
    const StorageSnapshotPtr & storage_snapshot,
    const MarkRanges & mark_ranges,
    UncompressedCache * /* uncompressed_cache */,
    MarkCache * /* mark_cache */,
    const AlterConversionsPtr & alter_conversions,
    const MergeTreeReaderSettings & reader_settings,
    const ValueSizeMap & /* avg_value_size_hints */,
    const ReadBufferFromFileBase::ProfileCallback & /* profile_callback */) const
{
    auto read_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(shared_from_this(), alter_conversions);
    auto ptr = std::static_pointer_cast<const MergeTreeDataPartInMemory>(shared_from_this());

    return std::make_unique<MergeTreeReaderInMemory>(
        read_info, ptr, columns_to_read, storage_snapshot, mark_ranges, reader_settings);
}

IMergeTreeDataPart::MergeTreeWriterPtr MergeTreeDataPartInMemory::getWriter(
    const NamesAndTypesList & columns_list,
    const StorageMetadataPtr & metadata_snapshot,
    const std::vector<MergeTreeIndexPtr> & /* indices_to_recalc */,
    const CompressionCodecPtr & /* default_codec */,
    const MergeTreeWriterSettings & writer_settings,
    const MergeTreeIndexGranularity & /* computed_index_granularity */)
{
    auto ptr = std::static_pointer_cast<MergeTreeDataPartInMemory>(shared_from_this());
    return std::make_unique<MergeTreeDataPartWriterInMemory>(
        ptr, columns_list, metadata_snapshot, writer_settings);
}

MutableDataPartStoragePtr MergeTreeDataPartInMemory::flushToDisk(const String & new_relative_path, const StorageMetadataPtr & metadata_snapshot) const
{
    auto reservation = storage.reserveSpace(block.bytes(), getDataPartStorage());
    VolumePtr volume = storage.getStoragePolicy()->getVolume(0);
    VolumePtr data_part_volume = createVolumeFromReservation(reservation, volume);

    auto new_data_part = storage.getDataPartBuilder(name, data_part_volume, new_relative_path)
        .withPartFormat(storage.choosePartFormatOnDisk(block.bytes(), rows_count))
        .build();

    auto new_data_part_storage = new_data_part->getDataPartStoragePtr();
    new_data_part_storage->beginTransaction();

    new_data_part->uuid = uuid;
    new_data_part->setColumns(columns, {}, metadata_snapshot->getMetadataVersion());
    new_data_part->partition.value = partition.value;
    new_data_part->minmax_idx = minmax_idx;

    if (new_data_part_storage->exists())
    {
        throw Exception(
            ErrorCodes::DIRECTORY_ALREADY_EXISTS,
            "Could not flush part {}. Part in {} already exists",
            quoteString(getDataPartStorage().getFullPath()),
            new_data_part_storage->getFullPath());
    }

    new_data_part_storage->createDirectories();

    auto compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);
    auto indices = MergeTreeIndexFactory::instance().getMany(metadata_snapshot->getSecondaryIndices());
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns, indices, compression_codec, NO_TRANSACTION_PTR);
    out.write(block);

    const auto & projections = metadata_snapshot->getProjections();
    for (const auto & [projection_name, projection] : projection_parts)
    {
        if (projections.has(projection_name))
        {
            auto old_projection_part = asInMemoryPart(projection);
            auto new_projection_part = new_data_part->getProjectionPartBuilder(projection_name)
                .withPartFormat(storage.choosePartFormatOnDisk(old_projection_part->block.bytes(), rows_count))
                .build();

            new_projection_part->is_temp = false; // clean up will be done on parent part
            new_projection_part->setColumns(projection->getColumns(), {}, metadata_snapshot->getMetadataVersion());

            auto new_projection_part_storage = new_projection_part->getDataPartStoragePtr();
            if (new_projection_part_storage->exists())
            {
                throw Exception(
                    ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                    "Could not flush projection part {}. Projection part in {} already exists",
                    projection_name,
                    new_projection_part_storage->getFullPath());
            }

            new_projection_part_storage->createDirectories();
            const auto & desc = projections.get(name);
            auto projection_compression_codec = storage.getContext()->chooseCompressionCodec(0, 0);
            auto projection_indices = MergeTreeIndexFactory::instance().getMany(desc.metadata->getSecondaryIndices());
            MergedBlockOutputStream projection_out(
                new_projection_part, desc.metadata,
                new_projection_part->getColumns(), projection_indices,
                projection_compression_codec, NO_TRANSACTION_PTR);

            projection_out.write(old_projection_part->block);
            projection_out.finalizePart(new_projection_part, false);
            new_data_part->addProjectionPart(projection_name, std::move(new_projection_part));
        }
    }

    out.finalizePart(new_data_part, false);
    new_data_part_storage->commitTransaction();
    return new_data_part_storage;
}

DataPartStoragePtr MergeTreeDataPartInMemory::makeCloneInDetached(const String & prefix,
                                                                  const StorageMetadataPtr & metadata_snapshot,
                                                                  const DiskTransactionPtr & disk_transaction) const
{
    if (disk_transaction)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "InMemory parts are not compatible with disk transactions");
    String detached_path = *getRelativePathForDetachedPart(prefix, /* broken */ false);
    return flushToDisk(detached_path, metadata_snapshot);
}

void MergeTreeDataPartInMemory::renameTo(const String & new_relative_path, bool /* remove_new_dir_if_exists */)
{
    getDataPartStorage().setRelativePath(new_relative_path);
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
    checksum.uncompressed_hash = getSipHash128AsPair(hash);
    return checksum;
}

DataPartInMemoryPtr asInMemoryPart(const MergeTreeDataPartPtr & part)
{
    return std::dynamic_pointer_cast<const MergeTreeDataPartInMemory>(part);
}
}
