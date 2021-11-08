#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class IMergedBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_);

    virtual ~IMergedBlockOutputStream() = default;

    using WrittenOffsetColumns = std::set<std::string>;

    virtual void write(const Block & block) = 0;

    const MergeTreeIndexGranularity & getIndexGranularity() const
    {
        return writer->getIndexGranularity();
    }

protected:
    // using SerializationState = ISerialization::SerializeBinaryBulkStatePtr;

    // ISerialization::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns);

    /// Remove all columns marked expired in data_part. Also, clears checksums
    /// and columns array. Return set of removed files names.
    static NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        MergeTreeData::DataPart::Checksums & checksums);

    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;

    VolumePtr volume;
    String part_path;

    IMergeTreeDataPart::MergeTreeWriterPtr writer;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
