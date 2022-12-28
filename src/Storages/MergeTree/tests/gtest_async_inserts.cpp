#include "Storages/MergeTree/ReplicatedMergeTreeSink.h"
#include "config.h"

#include <gtest/gtest.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>

 namespace DB {

std::vector<ChunkOffsetsPtr> scatterOffsetsBySelector(ChunkOffsetsPtr chunk_offsets, const IColumn::Selector & selector, size_t partition_num);

class AsyncInsertsTest : public ::testing::TestPartResult
{};


TEST(AsyncInsertsTest, testScatterOffsetsBySelector)
{
    auto test_impl = [](std::vector<size_t> offsets, std::vector<size_t> selector_data, size_t part_num, std::vector<std::vector<size_t>> expected)
    {
        auto offset_ptr = std::make_shared<ChunkOffsets>(offsets);
        IColumn::Selector selector(selector_data.size());
        size_t num_rows = selector_data.size();
        for (size_t i = 0; i < num_rows; i++)
            selector[i] = selector_data[i];

        auto results = scatterOffsetsBySelector(offset_ptr, selector, part_num);
        ASSERT_EQ(results.size(), expected.size());
        for (size_t i = 0; i < results.size(); i++)
        {
            auto result = results[i]->offsets;
            auto expect = expected[i];
            ASSERT_EQ(result.size(), expect.size());
            for (size_t j = 0; j < result.size(); j++)
                ASSERT_EQ(result[j], expect[j]);
        }
    };

    test_impl({5}, {0,1,0,1,0}, 2, {{3},{2}});
    test_impl({5,10}, {0,1,0,1,0,1,0,1,0,1}, 2, {{3,5},{2,5}});
    test_impl({4,8,12}, {0,1,0,1,0,2,0,2,1,2,1,2}, 3, {{2,4},{2,4},{2,4}});
    test_impl({1,2,3,4,5}, {0,1,2,3,4}, 5, {{1},{1},{1},{1},{1}});
    test_impl({3,6,10}, {1,1,1,2,2,2,0,0,0,0}, 3, {{4},{3},{3}});
}

std::vector<Int64> testSelfDeduplicate(std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes);

TEST(AsyncInsertsTest, testSelfDeduplicate)
{
    auto test_impl = [](std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes, std::vector<Int64> answer)
    {
        auto result = testSelfDeduplicate(data, offsets, hashes);
        ASSERT_EQ(answer.size(), result.size());
        for (size_t i = 0; i < result.size(); i++)
            ASSERT_EQ(answer[i], result[i]);
    };
    test_impl({1,2,3,1,2,3,4,5,6,1,2,3},{3,6,9,12},{"a","a","b","a"},{1,2,3,4,5,6});
    test_impl({1,2,3,1,2,3,1,2,3,1,2,3},{2,3,5,6,8,9,11,12},{"a","b","a","b","a","b","a","b"},{1,2,3});
    test_impl({1,2,3,1,2,4,1,2,5,1,2},{2,3,5,6,8,9,11},{"a","b","a","c","a","d","a"},{1,2,3,4,5});
    test_impl({1,2,1,2,1,2,1,2,1,2},{2,4,6,8,10},{"a","a","a","a","a"},{1,2});
}

}
