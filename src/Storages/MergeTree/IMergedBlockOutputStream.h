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
        /// written for the part, across all its output streams: each stream samples the blocks it
        /// writes into this builder (a vertical merge writes the merging columns through the
        /// horizontal stream and each gathered column through its own column-only stream). The owner
        /// of the part being written constructs the builder over the columns the streams will write.
        /// A column-only mutation additionally adds the counts carried over from the source part for
        /// the hardlinked columns (see `MutateSomePartColumnsTask`). The sampling needs no
        /// synchronization: the streams of one part never write concurrently (in a vertical merge the
        /// horizontal stage completes before the vertical one, which writes the gathered columns one
        /// at a time).
        EstimatesBuilder estimates_builder;
        /// False when the builder was already sampled upstream (inserts sample the whole block to
        /// choose the serialization kinds before the stream exists), so the streams must not count
        /// the same rows again.
        bool sample_written_blocks = true;
    };

    using GatheredDataPtr = std::shared_ptr<GatheredData>;

    IMergedBlockOutputStream(
        MergeTreeSettingsPtr storage_settings_,
        MutableDataPartStoragePtr data_part_storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        GatheredDataPtr gathered_data_,
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

    /// Created by the creator of the stream (e.g. `MergeTask`), shared between all output streams
    /// of the part being written and consumed when the part is finalized.
    GatheredDataPtr gathered_data;

    bool reset_columns = false;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
