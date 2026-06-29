#pragma once
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class LoadedMergeTreeDataPartInfoForReader final : public IMergeTreeDataPartInfoForReader
{
public:
    LoadedMergeTreeDataPartInfoForReader(
        MergeTreeData::DataPartPtr data_part_, AlterConversionsPtr alter_conversions_)
        : IMergeTreeDataPartInfoForReader(data_part_->storage.getContext())
        , data_part(std::move(data_part_))
        , alter_conversions(std::move(alter_conversions_))
    {
    }

    bool isCompactPart() const override { return DB::isCompactPart(data_part); }

    bool isWidePart() const override { return DB::isWidePart(data_part); }

    bool isProjectionPart() const override { return data_part->isProjectionPart(); }

    bool hasLightweightDelete() const override { return data_part->hasLightweightDelete(); }

    const String & getPartName() const override { return data_part->name; }

    const MergeTreePartInfo & getPartInfo() const override { return data_part->info; }

    Int64 getMinDataVersion() const override
    {
        return data_part->info.isPatch()
            ? data_part->getSourcePartsSet().getMinDataVersion()
            : data_part->info.getDataVersion();
    }

    Int64 getMaxDataVersion() const override
    {
        return data_part->info.isPatch()
            ? data_part->getSourcePartsSet().getMaxDataVersion()
            : data_part->info.getDataVersion();
    }

    IndexPtr getIndexPtr() const override { return data_part->getIndex(); }

    DataPartStoragePtr getDataPartStorage() const override { return data_part->getDataPartStoragePtr(); }

    const NamesAndTypesList & getColumns() const override { return data_part->getColumns(); }

    const ColumnsDescription & getColumnsDescription() const override { return data_part->getColumnsDescription(); }

    const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const override { return data_part->getColumnsDescriptionWithCollectedNested(); }

    const ColumnsSubstreams & getColumnsSubstreams() const override { return data_part->getColumnsSubstreams(); }

    std::optional<size_t> getColumnPosition(const String & column_name) const override { return data_part->getColumnPosition(column_name); }

    AlterConversionsPtr getAlterConversions() const override { return alter_conversions; }

    String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const override
    {
        return data_part->getColumnNameWithMinimumCompressedSize(available_columns);
    }

    const MergeTreeDataPartChecksums & getChecksums() const override { return data_part->checksums; }

    void reportBroken() override { data_part->storage.reportBrokenPart(data_part); }

    size_t getMarksCount() const override { return data_part->getMarksCount(); }

    size_t getFileSizeOrZero(const std::string & file_name) const override { return data_part->getFileSizeOrZero(file_name); }

    const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { return data_part->index_granularity_info; }

    const MergeTreeIndexGranularity & getIndexGranularity() const override { return *data_part->index_granularity; }

    const SerializationInfoByName & getSerializationInfos() const override { return data_part->getSerializationInfos(); }

    SerializationPtr getSerialization(const NameAndTypePair & column) const override { return data_part->getSerialization(column.name); }

    String getTableName() const override { return data_part->storage.getStorageID().getNameForLogs(); }

    MergeTreeData::DataPartPtr getDataPart() const { return data_part; }

    void setReadHints(const RangesInDataPartReadHints & read_hints_, const NamesAndTypesList & read_columns) override
    {
        if (read_columns.contains("_distance"))
            read_hints = read_hints_;
    }

    const RangesInDataPartReadHints & getReadHints() const override { return read_hints; }

    size_t getRowCount() const override { return data_part->rows_count; }
private:
    MergeTreeData::DataPartPtr data_part;
    AlterConversionsPtr alter_conversions;
    RangesInDataPartReadHints read_hints;
};

}
