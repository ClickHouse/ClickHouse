#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Interpreters/InsertDeduplication.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Common/PODArray.h>
#include <base/defines.h>

#include <cstddef>
#include <gtest/gtest.h>


 namespace DB {

std::vector<AsyncInsertInfoPtr> scatterAsyncInsertInfoBySelector(DeduplicationInfo::Ptr insert_info, const IColumn::Selector & selector, size_t partition_num);

class AsyncInsertsTest : public ::testing::TestPartResult
{};


std::vector<Int64> testSelfDeduplicate(std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes)
{
    MutableColumnPtr column = DataTypeInt64().createColumn();
    for (auto datum : data)
    {
        column->insert(datum);
    }
    Block block({ColumnWithTypeAndName(std::move(column), DataTypePtr(new DataTypeInt64()), "a")});

    auto deduplication_info = DeduplicationInfo::create(true);
    deduplication_info->setRootViewID({});
    deduplication_info->disabled = false; // there is no insert dependencies instance in this test
    deduplication_info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));

    chassert(offsets.size() == hashes.size());
    chassert(!offsets.empty());

    deduplication_info->setUserToken(hashes[0], offsets[0]);

    for (size_t i = 1; i < offsets.size(); ++i)
        deduplication_info->setUserToken(hashes[i], offsets[i] - offsets[i-1]);

    chassert(offsets.size() == deduplication_info->getCount());
    chassert(offsets.back() == deduplication_info->getRows());

    auto filtered = deduplication_info->filterImpl(deduplication_info->filterSelf("all"));

    ColumnPtr col = filtered.filtered_block->getColumns()[0];

    std::vector<Int64> result;
    result.reserve(col->size());

    for (size_t i = 0; i < col->size(); i++)
    {
        result.push_back(col->getInt(i));
    }

    return result;
}

TEST(AsyncInsertsTest, testSelfDeduplicate)
{
    auto test_impl = [](std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes, std::vector<Int64> answer)
    {
        auto result = testSelfDeduplicate(data, offsets, hashes);
        ASSERT_EQ(answer, result);
    };
    test_impl({1,2,3,1,2,3,4,5,6,1,2,3},{3,6,9,12},{"a","a","b","a"},{1,2,3,4,5,6});
    test_impl({1,2,3,1,2,3,1,2,3,1,2,3},{2,3,5,6,8,9,11,12},{"a","b","a","b","a","b","a","b"},{1,2,3});
    test_impl({1,2,3,1,2,4,1,2,5,1,2},{2,3,5,6,8,9,11},{"a","b","a","c","a","d","a"},{1,2,3,4,5});
    test_impl({1,2,1,2,1,2,1,2,1,2},{2,4,6,8,10},{"a","a","a","a","a"},{1,2});
}


/// Self-deduplication must be position-invariant for variable-length columns. With the unified hash
/// (NEW_UNIFIED_HASHES) the data hash is computed column-wise over a row range; if it folded in
/// absolute string/array offsets, two equal rows located at different offsets would get different
/// block ids and fail to deduplicate (e.g. repeated rows combined into one async insert).
std::vector<String> testSelfDeduplicateStrings(std::vector<String> data, std::vector<size_t> offsets, std::vector<String> hashes)
{
    MutableColumnPtr column = DataTypeString().createColumn();
    for (const auto & datum : data)
    {
        column->insert(datum);
    }
    Block block({ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "a")});

    auto deduplication_info = DeduplicationInfo::create(true);
    deduplication_info->setRootViewID({});
    deduplication_info->disabled = false; // there is no insert dependencies instance in this test
    deduplication_info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));

    chassert(offsets.size() == hashes.size());
    chassert(!offsets.empty());

    deduplication_info->setUserToken(hashes[0], offsets[0]);

    for (size_t i = 1; i < offsets.size(); ++i)
        deduplication_info->setUserToken(hashes[i], offsets[i] - offsets[i-1]);

    chassert(offsets.size() == deduplication_info->getCount());
    chassert(offsets.back() == deduplication_info->getRows());

    auto filtered = deduplication_info->filterImpl(deduplication_info->filterSelf("all"));

    /// Nothing was deduplicated — all rows survive in their original order.
    if (filtered.removed_rows == 0 || !filtered.filtered_block)
        return data;

    ColumnPtr col = filtered.filtered_block->getColumns()[0];

    std::vector<String> result;
    result.reserve(col->size());

    for (size_t i = 0; i < col->size(); i++)
    {
        result.push_back(String(col->getDataAt(i)));
    }

    return result;
}

TEST(AsyncInsertsTest, testSelfDeduplicateStrings)
{
    auto test_impl = [](std::vector<String> data, std::vector<size_t> offsets, std::vector<String> hashes, std::vector<String> answer)
    {
        auto result = testSelfDeduplicateStrings(data, offsets, hashes);
        ASSERT_EQ(answer, result);
    };
    /// Two equal single-row blocks with no user token must collapse to one row.
    test_impl({"one line","one line"},{1,2},{"",""},{"one line"});
    /// Equal multi-row blocks with no user token must collapse, keeping the first occurrence.
    test_impl({"a","bb","a","bb","ccc"},{2,4,5},{"","",""},{"a","bb","ccc"});
    /// Distinct blocks must survive (no false deduplication from relative offsets).
    test_impl({"ab","c","a","bc"},{2,4},{"",""},{"ab","c","a","bc"});
}


/// Verify that cloneSelf after prewarmDataHashes produces the same deduplication result as
/// operating on the original. This simulates the partition sink loop: the original
/// DeduplicationInfo is pre-warmed once, then cloned once per partition; each clone must
/// inherit the cached data_hash_batch and produce correct results without recomputing hashes.
std::vector<String> testPrewarmDataHashes(std::vector<String> data, std::vector<size_t> offsets)
{
    MutableColumnPtr column = DataTypeString().createColumn();
    for (const auto & datum : data)
        column->insert(datum);
    Block block({ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "a")});

    auto deduplication_info = DeduplicationInfo::create(true);
    deduplication_info->setRootViewID({});
    deduplication_info->disabled = false;
    deduplication_info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));

    /// Empty user token → data-hash path, which is exactly what prewarmDataHashes covers.
    deduplication_info->setUserToken("", offsets[0]);
    for (size_t i = 1; i < offsets.size(); ++i)
        deduplication_info->setUserToken("", offsets[i] - offsets[i - 1]);

    /// Pre-warm on the original, then clone — mirroring what MergeTreeSink / ReplicatedMergeTreeSink do.
    deduplication_info->prewarmDataHashes();
    auto clone = deduplication_info->cloneSelf();

    auto filtered = clone->filterImpl(clone->filterSelf("all"));

    if (filtered.removed_rows == 0 || !filtered.filtered_block)
        return data;

    ColumnPtr col = filtered.filtered_block->getColumns()[0];
    std::vector<String> result;
    result.reserve(col->size());
    for (size_t i = 0; i < col->size(); i++)
        result.push_back(String(col->getDataAt(i)));
    return result;
}

TEST(AsyncInsertsTest, testPrewarmDataHashes)
{
    auto test_impl = [](std::vector<String> data, std::vector<size_t> offsets, std::vector<String> answer)
    {
        auto result = testPrewarmDataHashes(data, offsets);
        ASSERT_EQ(answer, result);
    };
    /// Two equal single-row blocks: prewarm + clone must still deduplicate correctly.
    test_impl({"one line","one line"}, {1,2}, {"one line"});
    /// Equal multi-row blocks: first occurrence kept after clone.
    test_impl({"a","bb","a","bb","ccc"}, {2,4,5}, {"a","bb","ccc"});
    /// Distinct blocks: no false deduplication through the pre-warmed clone.
    test_impl({"ab","c","a","bc"}, {2,4}, {"ab","c","a","bc"});
    /// Three identical single-row blocks: only the first survives.
    test_impl({"x","x","x"}, {1,2,3}, {"x"});
}


/// Drive the real sink hot path: prewarm the batched block once, then split it per partition via
/// filterToPartition (which builds a per-partition DeduplicationInfo copy) and self-deduplicate each
/// partition. Returns surviving rows in (partition, row) order. With prewarm the cached hash is
/// carried into each per-partition copy; without it each copy recomputes — the result must match.
std::vector<String> testPrewarmFilterToPartition(
    std::vector<String> data, std::vector<size_t> token_offsets,
    std::vector<UInt64> row_to_partition, size_t num_partitions, bool prewarm)
{
    MutableColumnPtr column = DataTypeString().createColumn();
    for (const auto & datum : data)
        column->insert(datum);
    Block block({ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "a")});

    auto deduplication_info = DeduplicationInfo::create(true);
    deduplication_info->setRootViewID({});
    deduplication_info->disabled = false;
    deduplication_info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));

    /// Empty user token → data-hash path (the one prewarmDataHashes covers). One token per entry.
    deduplication_info->setUserToken("", token_offsets[0]);
    for (size_t i = 1; i < token_offsets.size(); ++i)
        deduplication_info->setUserToken("", token_offsets[i] - token_offsets[i - 1]);

    if (prewarm)
        deduplication_info->prewarmDataHashes();

    PaddedPODArray<UInt64> selector;
    for (auto p : row_to_partition)
        selector.push_back(p);

    std::vector<String> result;
    for (size_t p = 0; p < num_partitions; ++p)
    {
        /// partition_id does not affect empty-token grouping (data hash is partition-independent).
        auto part_info = deduplication_info->filterToPartition(selector, p);
        auto filtered = part_info->filterImpl(part_info->filterSelf("all"));

        const auto & out_block = (filtered.removed_rows == 0 || !filtered.filtered_block)
            ? part_info->original_block : filtered.filtered_block;
        if (!out_block)
            continue;
        ColumnPtr col = out_block->getColumns()[0];
        for (size_t i = 0; i < col->size(); ++i)
            result.push_back(String(col->getDataAt(i)));
    }
    return result;
}

TEST(AsyncInsertsTest, testPrewarmFilterToPartition)
{
    auto build = [](std::vector<String> data, std::vector<size_t> offsets, std::vector<UInt64> rtp, size_t parts)
    {
        auto warm = testPrewarmFilterToPartition(data, offsets, rtp, parts, /*prewarm=*/true);
        auto cold = testPrewarmFilterToPartition(data, offsets, rtp, parts, /*prewarm=*/false);
        /// The prewarm optimization must not change the deduplication result.
        EXPECT_EQ(warm, cold);
        return warm;
    };

    /// Single-row tokens across 2 partitions. p0 rows {0,1,4}=[A,A,C]->[A,C]; p1 rows {2,3,5}=[B,B,A]->[B,A].
    EXPECT_EQ(build({"A","A","B","B","C","A"}, {1,2,3,4,5,6}, {0,0,1,1,0,1}, 2),
              (std::vector<String>{"A","C","B","A"}));
    /// Multi-row tokens. p0 keeps tokens {0,1}=[A,X,A,X]->[A,X] (dup dropped); p1 keeps token{2}=[B,Y].
    EXPECT_EQ(build({"A","X","A","X","B","Y"}, {2,4,6}, {0,0,0,0,1,1}, 2),
              (std::vector<String>{"A","X","B","Y"}));
    /// All tokens in one partition: filterToPartition keeps everything, self-dedup collapses duplicates.
    EXPECT_EQ(build({"A","B","A","B","C"}, {1,2,3,4,5}, {0,0,0,0,0}, 1),
              (std::vector<String>{"A","B","C"}));
}


/// Verify prewarmDataHashes populates the per-token cache and that filterToPartition carries it into
/// the per-partition copies, while without prewarm those copies start cold (cache filled lazily later).
bool testPrewarmPopulatesCache(
    std::vector<String> data, std::vector<size_t> token_offsets,
    std::vector<UInt64> row_to_partition, size_t num_partitions)
{
    auto build = [&](bool prewarm)
    {
        MutableColumnPtr column = DataTypeString().createColumn();
        for (const auto & datum : data)
            column->insert(datum);
        Block block({ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "a")});

        auto info = DeduplicationInfo::create(true);
        info->setRootViewID({});
        info->disabled = false;
        info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));
        info->setUserToken("", token_offsets[0]);
        for (size_t i = 1; i < token_offsets.size(); ++i)
            info->setUserToken("", token_offsets[i] - token_offsets[i - 1]);
        if (prewarm)
            info->prewarmDataHashes();
        return info;
    };

    PaddedPODArray<UInt64> selector;
    for (auto p : row_to_partition)
        selector.push_back(p);

    auto warm = build(true);
    auto cold = build(false);
    for (size_t p = 0; p < num_partitions; ++p)
    {
        auto warm_part = warm->filterToPartition(selector, p);
        auto cold_part = cold->filterToPartition(selector, p);
        for (const auto & t : warm_part->tokens)
            if (t.by_user.empty() && !t.data_hash_batch.has_value())
                return false;
        for (const auto & t : cold_part->tokens)
            if (t.by_user.empty() && t.data_hash_batch.has_value())
                return false;
    }
    return true;
}

TEST(AsyncInsertsTest, testPrewarmPopulatesCache)
{
    EXPECT_TRUE(testPrewarmPopulatesCache({"A","A","B","B","C","A"}, {1,2,3,4,5,6}, {0,0,1,1,0,1}, 2));
    EXPECT_TRUE(testPrewarmPopulatesCache({"A","X","A","X","B","Y"}, {2,4,6}, {0,0,0,0,1,1}, 2));
}


/// A disabled info (async insert with async_insert_deduplicate=0, the default) reaches the sink prewarm
/// call because the guard tests the sink-level `deduplicate` (replicated_deduplication_window!=0), not
/// `disabled`; but deduplicateSelf/getDeduplicationHashes early-return, so warming hashes is wasted work.
/// Returns true iff prewarmDataHashes left every empty-token cache untouched on a disabled info.
bool testPrewarmDisabledIsNoop(std::vector<String> data, std::vector<size_t> token_offsets)
{
    MutableColumnPtr column = DataTypeString().createColumn();
    for (const auto & datum : data)
        column->insert(datum);
    Block block({ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "a")});

    auto info = DeduplicationInfo::create(true);
    info->setRootViewID({});
    /// Populate the block with dedup enabled, then flip to disabled to model the state the sink sees:
    /// block present, tokens set, but deduplication off (updateOriginalBlock skips a disabled info).
    info->disabled = false;
    info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));
    info->setUserToken("", token_offsets[0]);
    for (size_t i = 1; i < token_offsets.size(); ++i)
        info->setUserToken("", token_offsets[i] - token_offsets[i - 1]);
    info->disabled = true;

    info->prewarmDataHashes();

    for (const auto & t : info->tokens)
        if (t.by_user.empty() && t.data_hash_batch.has_value())
            return false;
    return true;
}

TEST(AsyncInsertsTest, testPrewarmDisabledIsNoop)
{
    EXPECT_TRUE(testPrewarmDisabledIsNoop({"A","A","B","B","C","A"}, {1,2,3,4,5,6}));
    EXPECT_TRUE(testPrewarmDisabledIsNoop({"A","X","A","X","B","Y"}, {2,4,6}));
}

}
