#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Info for reader which was constructed on-the-fly
/// according to some passed information, that is why it is called "borrowed".
class BorrowedMergeTreeDataPartInfoForReader final : public IMergeTreeDataPartInfoForReader
{
public:
    BorrowedMergeTreeDataPartInfoForReader(
        MergeTreeDataPartType type_,
        DataPartStoragePtr data_part_storage_,
        NamesAndTypesList columns_,
        ColumnsSubstreams columns_substreams_,
        NameSet invalidated_system_columns_,
        MergeTreeIndexGranularityInfo index_granularity_info_,
        MergeTreeIndexGranularityPtr index_granularity_,
        MergeTreeDataPartChecksums checksums_,
        SerializationInfoByName serialization_infos_,
        String table_name_,
        size_t marks_count_,
        MergeTreeSettingsPtr storage_settings_,
        ContextPtr context_,
        bool share_nested_offsets_ = true)
        : IMergeTreeDataPartInfoForReader(context_)
        , type(type_)
        , data_part_storage(std::move(data_part_storage_))
        , part_name(data_part_storage->getPartDirectory())
        , part_info(MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING))
        , columns(columns_)
        , columns_substreams(std::move(columns_substreams_))
        , invalidated_system_columns(std::move(invalidated_system_columns_))
        , columns_description(columns_)
        /// Mirror `MergeTreeData::getColumnsDescriptionForColumns`: collect dotted `Array` columns into a
        /// shared-offsets Nested structure only when `share_nested_offsets` is on; otherwise keep them independent.
        , columns_description_with_collected_nested(share_nested_offsets_ ? ColumnsDescription(Nested::collect(columns_)) : ColumnsDescription(columns_))
        , index_granularity_info(std::move(index_granularity_info_))
        , index_granularity(std::move(index_granularity_))
        , checksums(std::move(checksums_))
        , serialization_infos(std::move(serialization_infos_))
        , table_name(std::move(table_name_))
        , marks_count(marks_count_)
        , storage_settings(std::move(storage_settings_))
    {
        column_name_to_position.reserve(columns.size());
        size_t pos = 0;
        for (const auto & column : columns)
            column_name_to_position.emplace(column.name, pos++);
    }

    bool isCompactPart() const override { return type == MergeTreeDataPartType::Compact; }

    bool isWidePart() const override { return type == MergeTreeDataPartType::Wide; }

    bool isProjectionPart() const override { return false; }

    bool hasLightweightDelete() const override { return columns_description.has(RowExistsColumn::name); }

    const String & getPartName() const override { return part_name; }

    const MergeTreePartInfo & getPartInfo() const override { return part_info; }

    /// TODO: fix for patch parts
    Int64 getMinDataVersion() const override { return part_info.getDataVersion(); }
    Int64 getMaxDataVersion() const override { return part_info.getDataVersion(); }
    IndexPtr getIndexPtr() const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BorrowedMergeTreeDataPartInfoForReader doesn't have index"); }

    /// The partition value is not loaded for a borrowed part. Only the folding of partition predicates
    /// into skip-index conditions asks for it, and skip indexes are never read through a borrowed part.
    const MergeTreePartition & getPartition() const override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BorrowedMergeTreeDataPartInfoForReader doesn't have partition information"); }

    DataPartStoragePtr getDataPartStorage() const override { return data_part_storage; }

    const NamesAndTypesList & getColumns() const override { return columns; }

    const ColumnsSubstreams & getColumnsSubstreams() const override { return columns_substreams; }

    const ColumnsDescription & getColumnsDescription() const override { return columns_description; }

    const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const override { return columns_description_with_collected_nested; }

    AlterConversionsPtr getAlterConversions() const override { return std::make_shared<AlterConversions>(); }

    const MergeTreeDataPartChecksums & getChecksums() const override { return checksums; }

    std::optional<size_t> getColumnPosition(const String & column_name) const override
    {
        auto it = column_name_to_position.find(column_name);
        if (it == column_name_to_position.end())
            return {};
        return it->second;
    }

    std::optional<NameAndTypePair> tryGetColumn(const String & column_name) const override
    {
        return columns_description.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
    }

    bool isSystemColumnInvalidated(const String & column_name) const override { return invalidated_system_columns.contains(column_name); }

    String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList & available_columns) const override
    {
        /// No per-column size info on a borrowed part; any readable column works (caller guarantees non-empty).
        return available_columns.front().name;
    }

    /// Projection parts are never read through a borrowed part, so there is no parent part.
    String getParentPartName() const override { return {}; }

    /// A borrowed part has no size information, see the comment in `IMergeTreeDataPartInfoForReader`.
    ColumnSize getColumnSize(const String &) const override { return {}; }
    std::shared_ptr<const std::unordered_map<String, ColumnSize>> getColumnSizes() const override { return nullptr; }
    ColumnSize getSubcolumnSize(const String &) const override { return {}; }

    MergeTreeSettingsPtr getStorageSettings() const override { return storage_settings; }

    /// A borrowed part is not backed by an `IMergeTreeDataPart`.
    std::shared_ptr<const IMergeTreeDataPart> getDataPart() const override { return nullptr; }

    size_t getMarksCount() const override { return marks_count; }

    size_t getFileSizeOrZero(const std::string & file_name) const override
    {
        auto checksum = checksums.files.find(file_name);
        if (checksum == checksums.files.end())
            return 0;
        return checksum->second.file_size;
    }

    const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { return index_granularity_info; }

    const MergeTreeIndexGranularity & getIndexGranularity() const override { return *index_granularity; }

    const SerializationInfoByName & getSerializationInfos() const override { return serialization_infos; }

    SerializationPtr getSerialization(const NameAndTypePair & column) const override
    {
        auto it = serialization_infos.find(column.getNameInStorage());
        return it == serialization_infos.end()
            ? IDataType::getSerialization(column)
            : IDataType::getSerialization(column, *it->second);
    }

    String getTableName() const override { return table_name; }

    size_t getRowCount() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getRowCount not implemented for BorrowedMergeTreeDataPartInfoForReader");
    }

    void reportBroken() override {} /// Noop.

private:
    MergeTreeDataPartType type;
    DataPartStoragePtr data_part_storage;
    String part_name;
    MergeTreePartInfo part_info;
    NamesAndTypesList columns;
    ColumnsSubstreams columns_substreams;
    NameSet invalidated_system_columns;
    ColumnsDescription columns_description;
    ColumnsDescription columns_description_with_collected_nested;
    MergeTreeIndexGranularityInfo index_granularity_info;
    MergeTreeIndexGranularityPtr index_granularity;
    MergeTreeDataPartChecksums checksums;
    SerializationInfoByName serialization_infos;
    String table_name;
    size_t marks_count;
    MergeTreeSettingsPtr storage_settings;
    std::unordered_map<std::string, size_t> column_name_to_position;
};


}
