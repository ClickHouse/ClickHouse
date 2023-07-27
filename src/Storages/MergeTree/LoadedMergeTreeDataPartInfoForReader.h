#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class LoadedMergeTreeDataPartInfoForReader final : public IMergeTreeDataPartInfoForReader
{
public:
    explicit LoadedMergeTreeDataPartInfoForReader(MergeTreeData::DataPartPtr data_part_)
        : IMergeTreeDataPartInfoForReader(data_part_->storage.getContext())
        , data_part(data_part_)
    {
    }

    bool isCompactPart() const override { return DB::isCompactPart(data_part); }

    bool isWidePart() const override { return DB::isWidePart(data_part); }

    bool isInMemoryPart() const override { return DB::isInMemoryPart(data_part); }

    bool isProjectionPart() const override { return data_part->isProjectionPart(); }

    DataPartStoragePtr getDataPartStorage() const override { return data_part->getDataPartStoragePtr(); }

    const NamesAndTypesList & getColumns() const override { return data_part->getColumns(); }

    const ColumnsDescription & getColumnsDescription() const override { return data_part->getColumnsDescription(); }

    const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const override { return data_part->getColumnsDescriptionWithCollectedNested(); }

    std::optional<size_t> getColumnPosition(const String & column_name) const override { return data_part->getColumnPosition(column_name); }

    AlterConversions getAlterConversions() const override { return data_part->storage.getAlterConversionsForPart(data_part); }

    String getColumnNameWithMinimumCompressedSize(bool with_subcolumns) const override { return data_part->getColumnNameWithMinimumCompressedSize(with_subcolumns); }

    const MergeTreeDataPartChecksums & getChecksums() const override { return data_part->checksums; }

    void reportBroken() override { data_part->storage.reportBrokenPart(data_part); }

    size_t getMarksCount() const override { return data_part->getMarksCount(); }

    size_t getFileSizeOrZero(const std::string & file_name) const override { return data_part->getFileSizeOrZero(file_name); }

    const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { return data_part->index_granularity_info; }

    const MergeTreeIndexGranularity & getIndexGranularity() const override { return data_part->index_granularity; }

    const SerializationInfoByName & getSerializationInfos() const override { return data_part->getSerializationInfos(); }

    SerializationPtr getSerialization(const NameAndTypePair & column) const override { return data_part->getSerialization(column.name); }

private:
    MergeTreeData::DataPartPtr data_part;
};

}
