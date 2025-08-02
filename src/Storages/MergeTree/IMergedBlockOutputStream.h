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

struct PartLevelStatistics
{
    explicit PartLevelStatistics(bool need_update_) : need_update(need_update_)
    {
    }

    bool need_update = false;
    ColumnsStatistics explicit_stats;
    ColumnsStatistics stats_for_serialization;
    IMergeTreeDataPart::MinMaxIndexPtr minmax_idx;

    void addExplicitStats(ColumnsStatistics stats) { explicit_stats = std::move(stats); }
    void addStatsForSerialization(ColumnsStatistics stats) { stats_for_serialization = std::move(stats); }
    void addMinMaxIdx(IMergeTreeDataPart::MinMaxIndexPtr idx) { minmax_idx = std::move(idx); }
    void update(const Block & block, const StorageMetadataPtr & metadata_snapshot);
};

class IMergedBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        const MergeTreeSettingsPtr & storage_settings_,
        MutableDataPartStoragePtr data_part_storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const PartLevelStatistics & part_level_statistics_);

    virtual ~IMergedBlockOutputStream() = default;

    using WrittenOffsetColumns = std::set<std::string>;

    virtual void write(const Block & block) = 0;
    virtual void cancel() noexcept = 0;

    MergeTreeIndexGranularityPtr getIndexGranularity() const
    {
        return writer->getIndexGranularity();
    }

    PlainMarksByName releaseCachedMarks()
    {
        return writer ? writer->releaseCachedMarks() : PlainMarksByName{};
    }

    size_t getNumberOfOpenStreams() const
    {
        return writer->getNumberOfOpenStreams();
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
    PartLevelStatistics part_level_statistics;
};

using IMergedBlockOutputStreamPtr = std::shared_ptr<IMergedBlockOutputStream>;

}
