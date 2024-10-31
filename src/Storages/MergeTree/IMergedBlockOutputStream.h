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
        const MergeTreeSettingsPtr & storage_settings_,
        MutableDataPartStoragePtr data_part_storage_,
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

    PlainMarksByName releaseCachedMarks()
    {
        return writer->releaseCachedMarks();
    }

protected:

    /// Remove all columns marked expired in data_part. Also, clears checksums
    /// and columns array. Return set of removed files names.
    NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        SerializationInfoByName & serialization_infos,
        MergeTreeData::DataPart::Checksums & checksums);

    MergeTreeSettingsPtr storage_settings;
    LoggerPtr log;

    StorageMetadataPtr metadata_snapshot;

    MutableDataPartStoragePtr data_part_storage;
    MergeTreeDataPartWriterPtr writer;

    bool reset_columns = false;
    SerializationInfoByName new_serialization_infos;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
