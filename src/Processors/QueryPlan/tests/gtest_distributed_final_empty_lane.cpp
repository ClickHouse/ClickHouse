#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/MergeTreeFinalMerge.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

/// Minimal metadata of a table with a single column `k` and `ORDER BY k` -- a primary key that
/// `deriveReverseOrder` accepts, which is what the `needs_merge` lanes of a distributed `FINAL`
/// read require.
StorageMetadataPtr makeMetadata(ContextPtr context)
{
    StorageInMemoryMetadata metadata;

    ColumnsDescription columns;
    columns.add(ColumnDescription("k", std::make_shared<DataTypeUInt64>()));
    metadata.setColumns(columns);

    ASTPtr order_by_ast = make_intrusive<ASTIdentifier>("k");
    metadata.sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata.columns, {}, context);
    metadata.primary_key = metadata.sorting_key;

    return std::make_shared<const StorageInMemoryMetadata>(std::move(metadata));
}

}

/// A primary-key-range layer may be empty, and an initiator may serialize it as a `needs_merge`
/// lane anyway. The in-order lane getter creates one source per part, so such a lane yields an
/// empty pipe, which has no header. `buildDistributedFinalPipe` must skip it instead of throwing
/// `Cannot add simple transform to empty Pipe` on the first transform.
TEST(BuildDistributedFinalPipe, SkipsEmptyMergeLanes)
{
    tryRegisterFunctions();

    const auto & context_holder = getContext();
    auto context = Context::createCopy(context_holder.context);

    auto metadata = makeMetadata(context);

    auto header = std::make_shared<const Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "k")});

    auto read_lane = [&](const RangesInDataPartsDescription & marks) -> Pipe
    {
        if (marks.empty())
            return {};
        return Pipe(std::make_shared<SourceFromSingleChunk>(header));
    };

    /// A single empty merge lane: the whole task produces nothing.
    {
        std::vector<DistributedReadBucket> lanes;
        lanes.push_back({/*marks=*/ {}, /*needs_merge=*/ true, /*borders=*/ {}, /*index=*/ 0});

        std::optional<ActionsDAG> out_projection;
        Pipe result = buildDistributedFinalPipe(
            lanes, metadata, MergeTreeData::MergingParams{}, /*max_block_size_rows=*/ 8192,
            /*enable_vertical_final=*/ false, context, out_projection, read_lane, read_lane);

        EXPECT_TRUE(result.empty());
        EXPECT_FALSE(out_projection.has_value());
    }

    /// An empty merge lane next to a non-empty one: the empty lane is dropped, the other is read.
    {
        RangesInDataPartDescription part_marks;
        part_marks.info = MergeTreePartInfo::fromPartName("all_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        part_marks.ranges = MarkRanges{{0, 1}};

        RangesInDataPartsDescription non_empty_marks;
        non_empty_marks.push_back(std::move(part_marks));

        std::vector<DistributedReadBucket> lanes;
        lanes.push_back({/*marks=*/ {}, /*needs_merge=*/ true, /*borders=*/ {}, /*index=*/ 0});
        lanes.push_back({std::move(non_empty_marks), /*needs_merge=*/ true, /*borders=*/ {}, /*index=*/ 0});

        std::optional<ActionsDAG> out_projection;
        Pipe result = buildDistributedFinalPipe(
            lanes, metadata, MergeTreeData::MergingParams{}, /*max_block_size_rows=*/ 8192,
            /*enable_vertical_final=*/ false, context, out_projection, read_lane, read_lane);

        ASSERT_FALSE(result.empty());
        EXPECT_TRUE(result.getHeader().has("k"));
    }
}
