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
        /// The estimates (num_rows/num_defaults per column and subcolumn) of the columns whose data is
        /// not written through the stream that persists `serialization.json`: the gathered columns of a
        /// vertical merge (accumulated by `MergeTask`, consumed by `finalizePartOnDisk`) and the columns
        /// of a column-only mutation (populated via `getSerializationEstimates` and the counts carried
        /// over from the source part, consumed by `finalizeMutatedPart`).
        Estimates serialization_estimates;
    };

    virtual void write(const Block & block) = 0;
    virtual void cancel() noexcept = 0;

    /// The estimates of the data written through this stream, reconciled with the counts from the
    /// explicit statistics (`external_estimates`), for persisting in `serialization.json`.
    Estimates getSerializationEstimates(const Estimates & external_estimates);

    /// Reuse `builder`'s already-sampled counts for `serialization.json` instead of sampling the written
    /// blocks. Used by inserts, which sample the whole block upfront to choose the serialization kinds;
    /// re-sampling the same rows here would only duplicate that work.
    void setSerializationEstimatesBuilder(EstimatesBuilder builder)
    {
        estimates_builder = std::move(builder);
        sample_written_blocks = false;
    }

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
    /// Carries the estimates (num_rows/num_defaults) of the columns being written, so the counts
    /// persisted in `serialization.json` reflect the actually-written data. The kinds themselves are
    /// chosen upstream (in `MergeTreeDataWriter` for inserts, in `MergeTask` for merges).
    EstimatesBuilder estimates_builder;
    /// Whether the builder samples the written blocks itself; false when an upstream builder that has
    /// already sampled the data was handed over via `setSerializationEstimatesBuilder`.
    bool sample_written_blocks = true;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
