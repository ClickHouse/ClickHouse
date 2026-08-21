#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Interpreters/InsertDeduplication.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
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

    auto deduplication_info = DeduplicationInfo::create(true, InsertDeduplicationVersions::NEW_UNIFIED_HASHES);
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

}
