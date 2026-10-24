#include <gtest/gtest.h>

#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <stdexcept>

using namespace DB;

namespace
{

class EmptyDataPartInfoForReader final : public IMergeTreeDataPartInfoForReader
{
public:
    EmptyDataPartInfoForReader() : IMergeTreeDataPartInfoForReader(ContextPtr{}) { }

    [[nodiscard]] bool isCompactPart() const override { unexpectedCall(); }
    [[nodiscard]] bool isWidePart() const override { unexpectedCall(); }
    [[nodiscard]] bool isProjectionPart() const override { unexpectedCall(); }
    [[nodiscard]] bool hasLightweightDelete() const override { unexpectedCall(); }
    [[nodiscard]] const String & getPartName() const override { unexpectedCall(); }
    [[nodiscard]] const MergeTreePartInfo & getPartInfo() const override { unexpectedCall(); }
    [[nodiscard]] const MergeTreePartition & getPartition() const override { unexpectedCall(); }
    [[nodiscard]] Int64 getMinDataVersion() const override { unexpectedCall(); }
    [[nodiscard]] Int64 getMaxDataVersion() const override { unexpectedCall(); }
    [[nodiscard]] IndexPtr getIndexPtr() const override { unexpectedCall(); }
    [[nodiscard]] DataPartStoragePtr getDataPartStorage() const override { return {}; }
    [[nodiscard]] const NamesAndTypesList & getColumns() const override { unexpectedCall(); }
    [[nodiscard]] const ColumnsDescription & getColumnsDescription() const override { unexpectedCall(); }
    [[nodiscard]] const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const override { unexpectedCall(); }
    [[nodiscard]] const ColumnsSubstreams & getColumnsSubstreams() const override { unexpectedCall(); }
    [[nodiscard]] std::optional<size_t> getColumnPosition(const String &) const override { unexpectedCall(); }
    [[nodiscard]] std::optional<NameAndTypePair> tryGetColumn(const String &) const override { unexpectedCall(); }
    [[nodiscard]] bool isSystemColumnInvalidated(const String &) const override { unexpectedCall(); }
    [[nodiscard]] String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList &) const override { unexpectedCall(); }
    [[nodiscard]] String getParentPartName() const override { unexpectedCall(); }
    [[nodiscard]] ColumnSize getColumnSize(const String &) const override { unexpectedCall(); }
    [[nodiscard]] std::shared_ptr<const std::unordered_map<String, ColumnSize>> getColumnSizes() const override { unexpectedCall(); }
    [[nodiscard]] CompressionCodecPtr getDefaultCompressionCodec() const override { unexpectedCall(); }
    [[nodiscard]] ColumnSize getSubcolumnSize(const String &) const override { unexpectedCall(); }
    [[nodiscard]] MergeTreeSettingsPtr getStorageSettings() const override { unexpectedCall(); }
    [[nodiscard]] std::shared_ptr<const IMergeTreeDataPart> getDataPart() const override { unexpectedCall(); }
    [[nodiscard]] const MergeTreeDataPartChecksums & getChecksums() const override { unexpectedCall(); }
    [[nodiscard]] AlterConversionsPtr getAlterConversions() const override { unexpectedCall(); }
    [[nodiscard]] size_t getMarksCount() const override { unexpectedCall(); }
    [[nodiscard]] size_t getFileSizeOrZero(const std::string &) const override { unexpectedCall(); }
    [[nodiscard]] const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { unexpectedCall(); }
    [[nodiscard]] const MergeTreeIndexGranularity & getIndexGranularity() const override { unexpectedCall(); }
    [[nodiscard]] SerializationPtr getSerialization(const NameAndTypePair &) const override { unexpectedCall(); }
    [[nodiscard]] SerializationPtr tryGetSerialization(const String &) const override { unexpectedCall(); }
    [[nodiscard]] const SerializationInfoByName & getSerializationInfos() const override { unexpectedCall(); }
    [[nodiscard]] String getTableName() const override { unexpectedCall(); }
    void reportBroken() override { unexpectedCall(); }
    [[nodiscard]] size_t getRowCount() const override { unexpectedCall(); }

private:
    [[noreturn]] static void unexpectedCall()
    {
        throw std::logic_error("Unexpected data part access");
    }
};

}

TEST(MergeTreeMarksLoader, AsyncLoadWithoutCache)
{
    auto data_part = std::make_shared<EmptyDataPartInfoForReader>();
    MergeTreeSettings settings;
    MergeTreeIndexGranularityInfo index_granularity_info(
        settings,
        MarkType(/* adaptive_ */ false, /* compressed_ */ false, /* with_substreams_ */ false, MergeTreeDataPartType::Wide));
    ThreadPool thread_pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 1);

    MergeTreeMarksLoader loader(
        data_part,
        /* mark_cache_ */ nullptr,
        "empty.mrk",
        /* marks_count_ */ 0,
        index_granularity_info,
        /* save_marks_in_cache_ */ false,
        ReadSettings{},
        &thread_pool,
        /* num_columns_in_mark_ */ 1,
        /* use_streaming_compression_ */ false);

    loader.startAsyncLoad();
    auto marks = loader.loadMarks();

    EXPECT_EQ(marks->getNumColumns(), 1);
}
