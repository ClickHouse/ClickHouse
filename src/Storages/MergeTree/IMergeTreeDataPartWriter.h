#pragma once

#include <Columns/IColumn_fwd.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Formats/MarkInCompressedFile.h>


namespace DB
{

using IColumnPermutation = PaddedPODArray<size_t>;
struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

using WrittenOffsetSubstreams = std::set<std::string>;

Block getIndexBlockAndPermute(const Block & block, const Names & names, const IColumnPermutation * permutation);

Block permuteBlockIfNeeded(const Block & block, const IColumnPermutation * permutation);

/// Writes data part to disk in different formats.
/// Calculates and serializes primary and skip indices if needed.
class IMergeTreeDataPartWriter : private boost::noncopyable
{
public:
    IMergeTreeDataPartWriter(
        const String & data_part_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const VirtualsDescriptionPtr & virtual_columns_,
        const MergeTreeWriterSettings & settings_,
        MergeTreeIndexGranularityPtr index_granularity_);

    virtual ~IMergeTreeDataPartWriter();

    virtual void write(const Block & block, const IColumnPermutation * permutation) = 0;

    virtual void finalizeIndexGranularity() = 0;
    virtual void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) = 0;

    virtual void finish(bool sync) = 0;
    virtual void cancel() noexcept = 0;

    virtual size_t getNumberOfOpenStreams() const = 0;

    std::optional<Columns> releaseIndexColumns();

    PlainMarksByName releaseCachedMarks();
    PlainMarksByName releaseCachedIndexMarks();

    MergeTreeIndexGranularityPtr getIndexGranularity() const { return index_granularity; }
    MergeTreeWriterSettings getWriterSettings() const { return settings; }

    virtual const Block & getColumnsSample() const = 0;

    virtual const ColumnsSubstreams & getColumnsSubstreams() const = 0;

protected:
    SerializationPtr getSerialization(const String & column_name) const;

    ASTPtr getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;

    IDataPartStorage & getDataPartStorage() { return *data_part_storage; }

    const String data_part_name;
    /// Serializations for every columns and subcolumns by their names.
    const SerializationByName serializations;
    const MergeTreeIndexGranularityInfo index_granularity_info;
    const MergeTreeSettingsPtr storage_settings;
    const StorageMetadataPtr metadata_snapshot;
    const VirtualsDescriptionPtr virtual_columns;
    const NamesAndTypesList columns_list;
    const MergeTreeWriterSettings settings;
    const bool with_final_mark;

    MutableDataPartStoragePtr data_part_storage;
    MutableColumns index_columns;
    MergeTreeIndexGranularityPtr index_granularity;
    /// Marks that will be saved to cache on finish.
    PlainMarksByName cached_marks;
    /// Index marks (for secondary indices) that will be saved to cache on finish.
    PlainMarksByName cached_index_marks;
};

using MergeTreeDataPartWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;
using ColumnPositions = std::unordered_map<std::string, size_t>;

MergeTreeDataPartWriterPtr createMergeTreeDataPartWriter(
        MergeTreeDataPartType part_type,
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const ColumnPositions & column_positions,
        const StorageMetadataPtr & metadata_snapshot,
        const VirtualsDescriptionPtr & virtual_columns_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        MergeTreeIndexGranularityPtr computed_index_granularity,
        WrittenOffsetSubstreams * written_offset_substreams);

}
