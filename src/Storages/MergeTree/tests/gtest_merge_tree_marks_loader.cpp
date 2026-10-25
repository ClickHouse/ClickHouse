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

    bool isCompactPart() const override { unexpectedCall(); }
    bool isWidePart() const override { unexpectedCall(); }
    bool isProjectionPart() const override { unexpectedCall(); }
    bool hasLightweightDelete() const override { unexpectedCall(); }
    const String & getPartName() const override { unexpectedCall(); }
    const MergeTreePartInfo & getPartInfo() const override { unexpectedCall(); }
    const MergeTreePartition & getPartition() const override { unexpectedCall(); }
    Int64 getMinDataVersion() const override { unexpectedCall(); }
    Int64 getMaxDataVersion() const override { unexpectedCall(); }
    IndexPtr getIndexPtr() const override { unexpectedCall(); }
    DataPartStoragePtr getDataPartStorage() const override { return {}; }
    const NamesAndTypesList & getColumns() const override { unexpectedCall(); }
    const ColumnsDescription & getColumnsDescription() const override { unexpectedCall(); }
    const ColumnsDescription & getColumnsDescriptionWithCollectedNested() const override { unexpectedCall(); }
    const ColumnsSubstreams & getColumnsSubstreams() const override { unexpectedCall(); }
    std::optional<size_t> getColumnPosition(const String &) const override { unexpectedCall(); }
    std::optional<NameAndTypePair> tryGetColumn(const String &) const override { unexpectedCall(); }
    bool isSystemColumnInvalidated(const String &) const override { unexpectedCall(); }
    String getColumnNameWithMinimumCompressedSize(const NamesAndTypesList &) const override { unexpectedCall(); }
    String getParentPartName() const override { unexpectedCall(); }
    ColumnSize getColumnSize(const String &) const override { unexpectedCall(); }
    std::shared_ptr<const std::unordered_map<String, ColumnSize>> getColumnSizes() const override { unexpectedCall(); }
    CompressionCodecPtr getDefaultCompressionCodec() const override { unexpectedCall(); }
    ColumnSize getSubcolumnSize(const String &) const override { unexpectedCall(); }
    MergeTreeSettingsPtr getStorageSettings() const override { unexpectedCall(); }
    std::shared_ptr<const IMergeTreeDataPart> getDataPart() const override { unexpectedCall(); }
    const MergeTreeDataPartChecksums & getChecksums() const override { unexpectedCall(); }
    AlterConversionsPtr getAlterConversions() const override { unexpectedCall(); }
    size_t getMarksCount() const override { unexpectedCall(); }
    size_t getFileSizeOrZero(const std::string &) const override { unexpectedCall(); }
    const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { unexpectedCall(); }
    const MergeTreeIndexGranularity & getIndexGranularity() const override { unexpectedCall(); }
    SerializationPtr getSerialization(const NameAndTypePair &) const override { unexpectedCall(); }
    const SerializationInfoByName & getSerializationInfos() const override { unexpectedCall(); }
    String getTableName() const override { unexpectedCall(); }
    void reportBroken() override { unexpectedCall(); }
    size_t getRowCount() const override { unexpectedCall(); }

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
