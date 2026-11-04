#include <gtest/gtest.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/QueryConditionCacheReadRangesRefiner.h>

#include <stdexcept>

using namespace DB;

namespace
{

[[noreturn]] void unexpectedCall()
{
    throw std::logic_error("Unexpected data part access");
}

/// The refiner only looks up the part by its address, so the part is never dereferenced.
class FakeDataPartInfoForReader final : public IMergeTreeDataPartInfoForReader
{
public:
    explicit FakeDataPartInfoForReader(DataPartPtr part_) : IMergeTreeDataPartInfoForReader(ContextPtr{}), part(std::move(part_)) { }

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
    MergeTreePartMinMaxIndexPtr getMinMaxIndex() const override { unexpectedCall(); }
    DataPartStoragePtr getDataPartStorage() const override { unexpectedCall(); }
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
    std::shared_ptr<const IMergeTreeDataPart> getDataPart() const override { return part; }
    const MergeTreeDataPartChecksums & getChecksums() const override { unexpectedCall(); }
    const SecondaryIndexColumnTypes & getSecondaryIndexColumnTypes() const override { unexpectedCall(); }
    AlterConversionsPtr getAlterConversions() const override { unexpectedCall(); }
    size_t getMarksCount() const override { unexpectedCall(); }
    size_t getFileSizeOrZero(const std::string &) const override { unexpectedCall(); }
    const MergeTreeIndexGranularityInfo & getIndexGranularityInfo() const override { unexpectedCall(); }
    const MergeTreeIndexGranularity & getIndexGranularity() const override { unexpectedCall(); }
    SerializationPtr getSerialization(const NameAndTypePair &) const override { unexpectedCall(); }
    SerializationPtr tryGetSerialization(const String &) const override { unexpectedCall(); }
    const SerializationInfoByName & getSerializationInfos() const override { unexpectedCall(); }
    String getTableName() const override { unexpectedCall(); }
    void reportBroken() override { unexpectedCall(); }
    size_t getRowCount() const override { unexpectedCall(); }

private:
    DataPartPtr part;
};

/// Drops the given marks and accounts them like `IndexReadRangesRefiner`. Records what it was asked to refine.
class AccountingInnerRefiner final : public IMergeTreeReadRangesRefiner
{
public:
    AccountingInnerRefiner(MergeTreeIndexBuildContextPtr index_build_context_, MarkRange to_drop_)
        : index_build_context(std::move(index_build_context_)), to_drop(to_drop_)
    {
    }

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override
    {
        received.insert(received.end(), ranges.begin(), ranges.end());

        MarkRanges result;
        size_t dropped_marks = 0;
        for (const auto & range : ranges)
        {
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                if (mark >= to_drop.begin && mark < to_drop.end)
                    ++dropped_marks;
                else if (!result.empty() && result.back().end == mark)
                    result.back().end = mark + 1;
                else
                    result.emplace_back(mark, mark + 1);
            }
        }

        auto & remaining_marks = index_build_context->part_remaining_marks.at(info.part_index_in_query).value;
        if (dropped_marks && remaining_marks.fetch_sub(dropped_marks, std::memory_order_acq_rel) == dropped_marks)
            index_build_context->index_reader_pool->clear(info.part_index_in_query);
        return result;
    }

    mutable MarkRanges received;

private:
    const MergeTreeIndexBuildContextPtr index_build_context;
    const MarkRange to_drop;
};

constexpr size_t part_index = 0;
constexpr size_t part_marks = 100;

struct Fixture
{
    /// Never dereferenced: an address to key the part by.
    int part_storage = 0;
    DataPartPtr part{std::shared_ptr<void>{}, reinterpret_cast<const IMergeTreeDataPart *>(&part_storage)};
    MergeTreeReadTaskInfo info;
    MergeTreeIndexBuildContextPtr index_build_context;

    Fixture()
    {
        info.data_part_info = std::make_shared<FakeDataPartInfoForReader>(part);
        info.part_index_in_query = part_index;

        /// As on a follower that skipped its own index analysis: the remaining marks count the whole part.
        RangesByIndex read_ranges;
        read_ranges.emplace(part_index, SkipIndexReadInput{MarkRanges{{0, part_marks}}, {}, 0});
        PartRemainingMarks part_remaining_marks;
        part_remaining_marks.emplace(part_index, part_marks);
        auto pool = std::make_shared<MergeTreeIndexReadResultPool>(
            nullptr, std::make_shared<MergeTreeProjectionIndexReader>(ProjectionIndexReaderByName{}));
        index_build_context = std::make_shared<MergeTreeIndexBuildContext>(
            std::move(read_ranges), ProjectionRangesByIndex{}, std::move(pool), std::move(part_remaining_marks));

        /// The cached result of the part, which the last processed mark has to release.
        index_build_context->index_reader_pool->getOrBuildIndexReadResult(
            part_index, info.data_part_info, index_build_context->read_ranges.at(part_index), {}, nullptr, {}, {});
    }

    QueryConditionCacheReadRangesPtr allowedRanges(MarkRanges ranges, bool dropped_marks_are_counted) const
    {
        auto res = std::make_shared<QueryConditionCacheReadRanges>();
        res->parts.emplace(part.get(), QueryConditionCacheReadRanges::Entry{part, std::move(ranges)});
        res->dropped_marks_are_counted = dropped_marks_are_counted;
        return res;
    }

    size_t remainingMarks() const { return index_build_context->part_remaining_marks.at(part_index).value.load(); }
    bool hasIndexReadResult() const { return index_build_context->index_reader_pool->hasIndexReadResult(part_index); }

    /// What `MergeTreeIndexBuildContext::getPreparedIndexReadResult` does for a read task.
    void read(size_t marks) const
    {
        auto & remaining_marks = index_build_context->part_remaining_marks.at(part_index).value;
        if (remaining_marks.fetch_sub(marks, std::memory_order_acq_rel) == marks)
            index_build_context->index_reader_pool->clear(part_index);
    }
};

}

TEST(QueryConditionCacheReadRangesRefiner, IntersectsWithAllowedRanges)
{
    Fixture f;
    QueryConditionCacheReadRangesRefiner refiner(f.allowedRanges({{10, 20}, {30, 35}, {90, 100}}, true), nullptr, nullptr);

    EXPECT_EQ(refiner.refine(f.info, {{0, 15}, {18, 32}}), MarkRanges({{10, 15}, {18, 20}, {30, 32}}));
    EXPECT_EQ(refiner.refine(f.info, {{40, 50}}), MarkRanges{});
    EXPECT_EQ(refiner.refine(f.info, {{95, 100}}), MarkRanges({{95, 100}}));
}

TEST(QueryConditionCacheReadRangesRefiner, DroppedAndReadMarksReleaseIndexReadResult)
{
    Fixture f;
    QueryConditionCacheReadRangesRefiner refiner(f.allowedRanges({{0, 10}}, true), f.index_build_context, nullptr);

    /// The surviving marks are read first, so the last dropped mark processes the part.
    EXPECT_EQ(refiner.refine(f.info, {{0, 10}}), MarkRanges({{0, 10}}));
    f.read(10);
    EXPECT_EQ(f.remainingMarks(), size_t{90});
    EXPECT_TRUE(f.hasIndexReadResult());

    EXPECT_EQ(refiner.refine(f.info, {{10, 100}}), MarkRanges{});
    EXPECT_EQ(f.remainingMarks(), size_t{0});
    EXPECT_FALSE(f.hasIndexReadResult());
}

TEST(QueryConditionCacheReadRangesRefiner, ReadMarksAfterDroppedOnesReleaseIndexReadResult)
{
    Fixture f;
    QueryConditionCacheReadRangesRefiner refiner(f.allowedRanges({{0, 10}}, true), f.index_build_context, nullptr);

    EXPECT_EQ(refiner.refine(f.info, {{0, 100}}), MarkRanges({{0, 10}}));
    EXPECT_EQ(f.remainingMarks(), size_t{10});
    EXPECT_TRUE(f.hasIndexReadResult());

    f.read(10);
    EXPECT_FALSE(f.hasIndexReadResult());
}

TEST(QueryConditionCacheReadRangesRefiner, InnerRefinerDropsAreNotCountedTwice)
{
    Fixture f;
    auto inner = std::make_shared<AccountingInnerRefiner>(f.index_build_context, MarkRange{5, 10});
    QueryConditionCacheReadRangesRefiner refiner(f.allowedRanges({{0, 10}}, true), f.index_build_context, inner);

    EXPECT_EQ(refiner.refine(f.info, {{0, 100}}), MarkRanges({{0, 5}}));
    /// The inner refiner sees only what is left after this one.
    EXPECT_EQ(inner->received, MarkRanges({{0, 10}}));
    /// 90 dropped here, 5 by the inner refiner.
    EXPECT_EQ(f.remainingMarks(), size_t{5});
    EXPECT_TRUE(f.hasIndexReadResult());

    f.read(5);
    EXPECT_FALSE(f.hasIndexReadResult());
}

TEST(QueryConditionCacheReadRangesRefiner, ReplicaWithOwnAnalysisDoesNotAccountDroppedMarks)
{
    Fixture f;
    /// The remaining marks of a replica that analyzed the part itself count only its own analyzed ranges.
    f.index_build_context->part_remaining_marks.at(part_index).value = 10;
    QueryConditionCacheReadRangesRefiner refiner(f.allowedRanges({{0, 10}}, false), f.index_build_context, nullptr);

    EXPECT_EQ(refiner.refine(f.info, {{0, 100}}), MarkRanges({{0, 10}}));
    EXPECT_EQ(f.remainingMarks(), size_t{10});
    EXPECT_TRUE(f.hasIndexReadResult());

    f.read(10);
    EXPECT_FALSE(f.hasIndexReadResult());
}

TEST(QueryConditionCacheReadRangesRefiner, PartWithoutAllowedRangesIsNotRefined)
{
    Fixture f;
    auto other = std::make_shared<QueryConditionCacheReadRanges>();
    QueryConditionCacheReadRangesRefiner refiner(other, f.index_build_context, nullptr);

    EXPECT_EQ(refiner.refine(f.info, {{0, 100}}), MarkRanges({{0, 100}}));
    EXPECT_EQ(f.remainingMarks(), part_marks);
}
