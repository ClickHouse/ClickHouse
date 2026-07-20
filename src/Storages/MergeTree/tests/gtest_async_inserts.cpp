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


/// Build a DeduplicationInfo with the given tokens and return, per token,
/// {cold before prewarm, warm after prewarm on the original, warm on a fresh cloneSelf()}.
/// An empty hash string means "no user token" (data hash is used). This exercises the
/// per-partition reuse: the sinks clone the original once per partition, so a warm original
/// must make every clone inherit the cache instead of rehashing.
std::vector<bool> testPrewarmDataHashes(std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes)
{
    MutableColumnPtr column = DataTypeInt64().createColumn();
    for (auto datum : data)
        column->insert(datum);
    Block block({ColumnWithTypeAndName(std::move(column), DataTypePtr(new DataTypeInt64()), "a")});

    auto deduplication_info = DeduplicationInfo::create(true);
    deduplication_info->setRootViewID({});
    deduplication_info->disabled = false; // there is no insert dependencies instance in this test
    deduplication_info->updateOriginalBlock(Chunk(block.getColumns(), block.rows()), std::make_shared<const Block>(block.cloneEmpty()));

    chassert(offsets.size() == hashes.size());
    chassert(!offsets.empty());

    deduplication_info->setUserToken(hashes[0], offsets[0]);
    for (size_t i = 1; i < offsets.size(); ++i)
        deduplication_info->setUserToken(hashes[i], offsets[i] - offsets[i - 1]);

    chassert(offsets.size() == deduplication_info->getCount());

    std::vector<bool> result;
    result.reserve(hashes.size() * 3);

    /// (a) cold before prewarm
    for (size_t i = 0; i < hashes.size(); ++i)
        result.push_back(deduplication_info->tokens[i].data_hash_batch.has_value());

    deduplication_info->prewarmDataHashes();

    /// (b) warm on the original after prewarm
    for (size_t i = 0; i < hashes.size(); ++i)
        result.push_back(deduplication_info->tokens[i].data_hash_batch.has_value());

    /// (c) warm on a fresh clone (per-partition clones must inherit the cache)
    auto clone = deduplication_info->cloneSelf();
    for (size_t i = 0; i < hashes.size(); ++i)
        result.push_back(clone->tokens[i].data_hash_batch.has_value());

    return result;
}

TEST(AsyncInsertsTest, testPrewarmDataHashes)
{
    /// Three no-user-token tokens (getCount() > 1): all cold before, all warm after, clone inherits.
    {
        auto r = testPrewarmDataHashes({1, 2, 3, 4, 5, 6}, {2, 4, 6}, {"", "", ""});
        ASSERT_EQ(r, (std::vector<bool>{
            false, false, false, // cold before
            true,  true,  true,  // warm after prewarm
            true,  true,  true})); // clone inherits the warm cache
    }
    /// Mixed: user-token tokens must NOT be warmed (they never hash the data); only the
    /// data tokens get cached, and the clone inherits exactly that set.
    {
        auto r = testPrewarmDataHashes({1, 2, 3, 4, 5, 6}, {2, 4, 6}, {"u", "", "u"});
        ASSERT_EQ(r, (std::vector<bool>{
            false, false, false,
            false, true,  false,
            false, true,  false}));
    }
    /// A single no-user-token token that spans several partitions is rehashed once per
    /// partition on the commit path, so it must be warmed too (getCount()==1 is in scope).
    {
        auto r = testPrewarmDataHashes({1, 2, 3}, {3}, {""});
        ASSERT_EQ(r, (std::vector<bool>{false, true, true}));
    }
    /// A single user-token token has nothing to warm (data hash is never used).
    {
        auto r = testPrewarmDataHashes({1, 2, 3}, {3}, {"u"});
        ASSERT_EQ(r, (std::vector<bool>{false, false, false}));
    }
}

}
