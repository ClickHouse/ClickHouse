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

    [[nodiscard]] bool isCompactPart() const override { return DB::isCompactPart(data_part); }

    [[nodiscard]] bool isWidePart() const override { return DB::isWidePart(data_part); }

    [[nodiscard]] bool isProjectionPart() const override { return data_part->isProjectionPart(); }

    [[nodiscard]] bool hasLightweightDelete() const override { return data_part->hasLightweightDelete(); }

    [[nodiscard]] const String & getPartName() const override { return data_part->name; }

    [[nodiscard]] const MergeTreePartInfo & getPartInfo() const override { return data_part->info; }

    [[nodiscard]] const MergeTreePartition & getPartition() const override { return data_part->partition; }

    [[nodiscard]] Int64 getMinDataVersion() const override
    {
        return data_part->info.isPatch()
            ? data_part->getPatchPartIndex().getMinDataVersion()
            : data_part->info.getDataVersion();
    }

    [[nodiscard]] Int64 getMaxDataVersion() const override
    {
        return data_part->info.isPatch()
            ? data_part->getPatchPartIndex().getMaxDataVersion()
            : data_part->info.getDataVersion();
    }

    [[nodiscard]] IndexPtr getIndexPtr() const override { return data_part->getIndex(); }

    [[nodiscard]] DataPartStoragePtr getDataPartStorage() const override { return data_part->getDataPartStoragePtr(); }

    [[nodiscard]] const NamesAndTypesList & getColumns() const override { return data_part->getColumns(); }

    [[nodiscard]] const ColumnsDescription & getColumnsDescription() const override { return data_part->getColumnsDescription(); }

    [[nodiscard]] const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const override { return data_part->getColumnsDescriptionWithCollectedNested(); }

    [[nodiscard]] const ColumnsSubstreams & getColumnsSubstreams() const override { return data_part->getColumnsSubstreams(); }

    [[nodiscard]] std::optional<size_t> getColumnPosition(const String & column_name) const override { return data_part->getColumnPosition(column_name); }

    [[nodiscard]] std::optional<NameAndTypePair> tryGetColumn(const String & column_name) const override { return data_part->tryGetColumn(column_name); }

    [[nodiscard]] bool isSystemColumnInvalidated(const String & column_name) const override { return data_part->isSystemColumnInvalidated(column_name); }

    [[nodiscard]] AlterConversionsPtr getAlterConversions() const override { return alter_conversions; }

    [[nodiscard]] String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const override
    {
        return data_part->getColumnNameWithMinimumCompressedSize(available_columns);
    }

    [[nodiscard]] String getParentPartName() const override { return data_part->getParentPartName(); }

    [[nodiscard]] ColumnSize getColumnSize(const String & column_name) const override { return data_part->getColumnSize(column_name); }

    [[nodiscard]] std::shared_ptr<const std::unordered_map<String, ColumnSize>> getColumnSizes() const override
    {
        return data_part->getColumnSizes();
    }

    [[nodiscard]] CompressionCodecPtr getDefaultCompressionCodec() const override { return data_part->default_codec; }

    [[nodiscard]] ColumnSize getSubcolumnSize(const String & subcolumn_name) const override { return data_part->getSubcolumnSize(subcolumn_name); }

    [[nodiscard]] const MergeTreeDataPartChecksums & getChecksums() const override { return data_part->checksums; }

    void reportBroken() override { data_part->storage.reportBrokenPart(data_part); }

    [[nodiscard]] size_t getMarksCount() const override { return data_part->getMarksCount(); }

    [[nodiscard]] size_t getFileSizeOrZero(const std::string & file_name) const override { return data_part->getFileSizeOrZero(file_name); }

    [[nodiscard]] const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { return data_part->index_granularity_info; }

    [[nodiscard]] const MergeTreeIndexGranularity & getIndexGranularity() const override { return *data_part->index_granularity; }

    [[nodiscard]] const SerializationInfoByName & getSerializationInfos() const override { return data_part->getSerializationInfos(); }

    [[nodiscard]] SerializationPtr getSerialization(const NameAndTypePair & column) const override { return data_part->getSerialization(column.name); }

    [[nodiscard]] SerializationPtr tryGetSerialization(const String & column_name) const override { return data_part->tryGetSerialization(column_name); }

    [[nodiscard]] SerializationPtr tryGetSubcolumnSerialization(const String & column_name, const String & subcolumn_name) const override
    {
        return data_part->tryGetSubcolumnSerialization(column_name, subcolumn_name);
    }

    [[nodiscard]] String getTableName() const override { return data_part->storage.getStorageID().getNameForLogs(); }

    [[nodiscard]] UUID getTableUUID() const override { return data_part->storage.getStorageID().uuid; }

    [[nodiscard]] MergeTreeSettingsPtr getStorageSettings() const override { return data_part->storage.getSettings(); }

    [[nodiscard]] std::shared_ptr<const IMergeTreeDataPart> getDataPart() const override { return data_part; }

    [[nodiscard]] size_t getRowCount() const override { return data_part->rows_count; }
private:
    MergeTreeData::DataPartPtr data_part;
    AlterConversionsPtr alter_conversions;
};

}
