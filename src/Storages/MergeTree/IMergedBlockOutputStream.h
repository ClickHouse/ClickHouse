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
        DataPartStorageBuilderPtr data_part_storage_builder_,
        const MergeTreeDataPartPtr & data_part,
        const StorageMetadataPtr & metadata_snapshot_,
        const NamesAndTypesList & columns_list,
        bool reset_columns_);

    virtual ~IMergedBlockOutputStream() = default;

    using WrittenOffsetColumns = std::set<std::string>;

    virtual void write(const Block & block) = 0;

    const MergeTreeIndexGranularity & getIndexGranularity() const
    {
        return writer->getIndexGranularity();
    }

protected:

    /// Remove all columns marked expired in data_part. Also, clears checksums
    /// and columns array. Return set of removed files names.
    NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        SerializationInfoByName & serialization_infos,
        MergeTreeData::DataPart::Checksums & checksums);

    const MergeTreeData & storage;
    StorageMetadataPtr metadata_snapshot;

    DataPartStorageBuilderPtr data_part_storage_builder;
    IMergeTreeDataPart::MergeTreeWriterPtr writer;

    bool reset_columns = false;
    SerializationInfoByName new_serialization_infos;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
