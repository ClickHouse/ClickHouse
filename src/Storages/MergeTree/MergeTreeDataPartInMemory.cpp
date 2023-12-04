#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeReaderInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <DataTypes/NestedUtils.h>
#include <Disks/createVolume.h>
#include <Interpreters/Context.h>
#include <Poco/Logger.h>


namespace DB
{

namespace ErrorCodes
{
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
    const NamesAndTypesList &,
    const StorageMetadataPtr &,
    const std::vector<MergeTreeIndexPtr> &,
    const CompressionCodecPtr &,
    const MergeTreeWriterSettings &,
    const MergeTreeIndexGranularity &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "In-memory data parts are obsolete and no longer supported for writing");
}

MutableDataPartStoragePtr MergeTreeDataPartInMemory::flushToDisk(const String &, const StorageMetadataPtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "In-memory data parts are obsolete and no longer supported for writing");
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
