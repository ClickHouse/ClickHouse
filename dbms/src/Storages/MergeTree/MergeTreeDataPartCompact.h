#pragma once

#include <Core/Row.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Columns/IColumn.h>

#include <Poco/Path.h>

#include <shared_mutex>


namespace DB
{

struct ColumnSize;
class MergeTreeData;


/// Description of the data part.
class MergeTreeDataPartCompact : public IMergeTreeDataPart
{
public:
    using Checksums = MergeTreeDataPartChecksums;
    using Checksum = MergeTreeDataPartChecksums::Checksum;

    MergeTreeDataPartCompact(
        const MergeTreeData & storage_,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeDataPartCompact( 
        MergeTreeData & storage_,
        const String & name_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const DiskSpace::DiskPtr & disk_,
        const std::optional<String> & relative_path_ = {});

    MergeTreeReaderPtr getReader(
        const NamesAndTypesList & columns,
        const MarkRanges & mark_ranges,
        UncompressedCache * uncompressed_cache,
        MarkCache * mark_cache,
        const ReaderSettings & reader_settings_,
        const ValueSizeMap & avg_value_size_hints = ValueSizeMap{},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback = ReadBufferFromFileBase::ProfileCallback{}) const override;
    
    MergeTreeWriterPtr getWriter(
        const NamesAndTypesList & columns_list,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec_,
        const WriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity = {}) const override;

    bool isStoredOnDisk() const override { return true; }

    /// Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
    /// If no checksums are present returns the name of the first physically existing column.
    String getColumnNameWithMinumumCompressedSize() const override;

    virtual Type getType() const override { return Type::COMPACT; }

    void checkConsistency(bool /* require_part_metadata */) const override {}

    ~MergeTreeDataPartCompact() override;

private:
    /// Loads marks index granularity into memory
    void loadIndexGranularity() override;

    ColumnSize getColumnSizeImpl(const String & name, const IDataType & type, std::unordered_set<String> * processed_substreams) const override;

    void checkConsistency(bool require_part_metadata);
};


// using MergeTreeDataPartState =IMergeTreeDataPart::State;

}
