#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Formats/MarkInCompressedFile.h>


namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

Block getIndexBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation);

Block permuteBlockIfNeeded(const Block & block, const IColumn::Permutation * permutation);

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
        const MergeTreeIndexGranularity & index_granularity_ = {});

    virtual ~IMergeTreeDataPartWriter();

    virtual void write(const Block & block, const IColumn::Permutation * permutation) = 0;

    virtual void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) = 0;

    virtual void finish(bool sync) = 0;

    Columns releaseIndexColumns();

    PlainMarksByName releaseCachedMarks();

    const MergeTreeIndexGranularity & getIndexGranularity() const { return index_granularity; }

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
    MergeTreeIndexGranularity index_granularity;
    /// Marks that will be saved to cache on finish.
    PlainMarksByName cached_marks;
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
        const ColumnsStatistics & stats_to_recalc_,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity);

}
