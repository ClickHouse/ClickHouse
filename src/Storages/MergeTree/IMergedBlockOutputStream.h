#pragma once

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <DataTypes/Serializations/EstimatesBuilder.h>
#include <Storages/Statistics/Statistics.h>
#include <Common/Logger.h>

namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

class IMergedBlockOutputStream
{
public:
    struct GatheredData
    {
        MergeTreeData::DataPart::Checksums checksums;
        ColumnsSubstreams columns_substreams;
        ColumnsStatistics statistics;
        /// Accumulates the estimates (num_rows/num_defaults per column and subcolumn) of all data
        /// written for the part. The owner of the part constructs the builder over the columns that
        /// will be written (excluding the columns whose exact counts the explicit statistics provide)
        /// and samples every block at the write call sites — a vertical merge writes the merging
        /// columns through the horizontal stream and each gathered column through its own
        /// column-only stream, all sampled into this one builder; inserts sample the whole block
        /// upfront to choose the serialization kinds. A column-only mutation additionally adds the
        /// counts carried over from the source part for the hardlinked columns (see
        /// `MutateSomePartColumnsTask`). The accumulated counts are persisted in
        /// `serialization.json` when the part is finalized.
        EstimatesBuilder estimates_builder;
    };

    IMergedBlockOutputStream(
        MergeTreeSettingsPtr storage_settings_,
        MutableDataPartStoragePtr data_part_storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        bool reset_columns_);

    virtual ~IMergedBlockOutputStream() = default;

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
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
