#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Common/Logger.h>

namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

class IMergedBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        MergeTreeSettingsPtr storage_settings_,
        MutableDataPartStoragePtr data_part_storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const NamesAndTypesList & columns_list,
        bool reset_columns_);

    virtual ~IMergedBlockOutputStream() = default;


    struct GatheredData
    {
        MergeTreeData::DataPart::Checksums checksums;
        ColumnsSubstreams columns_substreams;
        ColumnsStatistics statistics;
    };

    virtual void write(const Block & block) = 0;
    virtual void cancel() noexcept = 0;

    MergeTreeIndexGranularityPtr getIndexGranularity() const
    {
        return writer->getIndexGranularity();
    }

    MergeTreeWriterSettings getWriterSettings() const
    {
        return writer->getWriterSettings();
    }

    PlainMarksByName releaseCachedMarks()
    {
        return writer ? writer->releaseCachedMarks() : PlainMarksByName{};
    }

    PlainMarksByName releaseCachedIndexMarks()
    {
        return writer ? writer->releaseCachedIndexMarks() : PlainMarksByName{};
    }

    size_t getNumberOfOpenStreams() const
    {
        return writer->getNumberOfOpenStreams();
    }

protected:
    /// Remove columns and/or narrow named-`Tuple` types of columns. Clears matching
    /// checksum entries and updates @columns / @serialization_infos. Returns the set
    /// of files that should be removed from disk.
    ///
    /// - @empty_columns: top-level column names whose entire contents should be removed.
    ///   (Used by the TTL whole-column expiry path and by horizontal-merge missing-column
    ///   detection.)
    /// - @expired_subfields_by_column: per-top-level-column dotted subfield paths whose
    ///   leaf streams should be removed while the surrounding `Tuple` type is narrowed.
    ///   The map key disambiguates ownership when two columns share a dotted prefix
    ///   (e.g. column `data` Tuple(deeper Tuple(y)) vs column `data.deeper` Tuple(y)),
    ///   so the same `data.deeper.y` path can correctly belong to either.
    ///
    /// If @removed_unhashed_stream_names is non-null, the unhashed substream names
    /// (as stored in `columns_substreams.txt`) are appended into it. The caller uses
    /// these to keep `columns_substreams.txt` consistent with the actual on-disk files.
    NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        const NameSet & empty_columns,
        const std::map<String, NameSet> & expired_subfields_by_column,
        SerializationInfoByName & serialization_infos,
        MergeTreeData::DataPart::Checksums & checksums,
        NameSet * removed_unhashed_stream_names = nullptr);

    MergeTreeSettingsPtr storage_settings;
    LoggerPtr log;

    StorageMetadataPtr metadata_snapshot;

    MutableDataPartStoragePtr data_part_storage;
    MergeTreeDataPartWriterPtr writer;

    bool reset_columns = false;
    SerializationInfo::Settings info_settings;
    SerializationInfoByName new_serialization_infos{{}};
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
