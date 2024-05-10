#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Disks/IDisk.h>
#include "Storages/MergeTree/MergeTreeDataPartType.h"
#include "Storages/MergeTree/MergeTreeSettings.h"


namespace DB
{

Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation);

Block permuteBlockIfNeeded(const Block & block, const IColumn::Permutation * permutation);

/// Writes data part to disk in different formats.
/// Calculates and serializes primary and skip indices if needed.
class IMergeTreeDataPartWriter : private boost::noncopyable
{
public:
    IMergeTreeDataPartWriter(
//        const MergeTreeMutableDataPartPtr & data_part_,

        const String & data_part_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,

        const MergeTreeSettingsPtr & storage_settings_,

        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeWriterSettings & settings_,
        const MergeTreeIndexGranularity & index_granularity_ = {});

    virtual ~IMergeTreeDataPartWriter();

    virtual void write(const Block & block, const IColumn::Permutation * permutation) = 0;

    virtual void fillChecksums(IMergeTreeDataPart::Checksums & checksums, NameSet & checksums_to_remove) = 0;

    virtual void finish(bool sync) = 0;

    Columns releaseIndexColumns();
    const MergeTreeIndexGranularity & getIndexGranularity() const { return index_granularity; }

protected:
    SerializationPtr getSerialization(const String & column_name) const;

    ASTPtr getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const;

    IDataPartStorage & getDataPartStorage() { return *data_part_storage; }


//    const MergeTreeMutableDataPartPtr data_part;  // TODO: remove

    /// Serializations for every columns and subcolumns by their names.
    String data_part_name;
    SerializationByName serializations;
    MutableDataPartStoragePtr data_part_storage;
    MergeTreeIndexGranularityInfo index_granularity_info;


//    const MergeTreeData & storage; // TODO: remove

    const MergeTreeSettingsPtr storage_settings;


    const StorageMetadataPtr metadata_snapshot;
    const NamesAndTypesList columns_list;
    const MergeTreeWriterSettings settings;
    MergeTreeIndexGranularity index_granularity;
    const bool with_final_mark;

    MutableColumns index_columns;
};

using MergeTreeDataPartWriterPtr = std::unique_ptr<IMergeTreeDataPartWriter>;

MergeTreeDataPartWriterPtr createMergeTreeDataPartWriter(
        MergeTreeDataPartType part_type,
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,

        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const Statistics & stats_to_recalc_,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec_,
        const MergeTreeWriterSettings & writer_settings,
        const MergeTreeIndexGranularity & computed_index_granularity);


}
