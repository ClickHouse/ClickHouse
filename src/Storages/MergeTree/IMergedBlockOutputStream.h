#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <DataTypes/Serializations/EstimatesBuilder.h>
#include <Common/Logger.h>

#include <optional>

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
        /// The estimates (num_rows/num_defaults per column and subcolumn) to persist in
        /// `serialization.json`, when the file is written outside the output stream (column-only
        /// mutations). Populated via `getSerializationEstimates`.
        Estimates serialization_estimates;
    };

    virtual void write(const Block & block) = 0;
    virtual void cancel() noexcept = 0;

    /// The estimates of the written data reconciled with the exact counts from the explicit statistics
    /// (`external_estimates`), for persisting in `serialization.json`.
    Estimates getSerializationEstimates(const Estimates & external_estimates);

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

    /// See IMergeTreeDataPartWriter::getSkipIndicesPackedWriter.
    class PackedFilesWriter * getSkipIndicesPackedWriter()
    {
        return writer ? writer->getSkipIndicesPackedWriter() : nullptr;
    }

protected:
    /// Remove all columns in @empty_columns. Also, clears checksums
    /// and columns array. Return set of removed files names.
    NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        const NameSet & empty_columns,
        SerializationInfoByName & serialization_infos,
        MergeTreeData::DataPart::Checksums & checksums);

    MergeTreeSettingsPtr storage_settings;
    LoggerPtr log;

    StorageMetadataPtr metadata_snapshot;

    MutableDataPartStoragePtr data_part_storage;
    MergeTreeDataPartWriterPtr writer;

    bool reset_columns = false;
    SerializationInfo::Settings info_settings;
    /// Samples the estimates (num_rows/num_defaults) of the columns being written, so the counts
    /// persisted in `serialization.json` reflect the actually-written data. Always present; the kinds
    /// themselves are chosen upstream (in `MergeTreeDataWriter` for inserts, in `MergeTask` for merges).
    std::optional<EstimatesBuilder> estimates_builder;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
