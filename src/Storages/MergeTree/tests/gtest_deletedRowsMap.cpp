#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeDataPartDeletedMask.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/MergeTree/iostream_debug_helpers.h>

#include <base/iostream_debug_helpers.h>

namespace DB
{
bool operator==(const MergeTreeDataPartDeletedMask & left, const MergeTreeDataPartDeletedMask & right)
{
    if (!!left.deleted_rows == !!right.deleted_rows)
    {
        if (!!left.deleted_rows)
            return left.deleted_rows->getData() == right.deleted_rows->getData();
        return true;
    }
    return false;
}

}

using namespace DB;

using GeneratorFunc = MergeTreeDataPartDeletedMask (*)(size_t min_size);

MergeTreeDataPartDeletedMask generateDeletedRows(size_t size)
{
    const std::vector<std::pair<size_t, std::vector<uint8_t>>> sequences =
    {
        {3,   {1, 0, 1, 0, 1, 0, 1, 0}},
        {100, {1}},
        {100, {0}},
        // 1 on positions of prime numbers (with leading 0): 1 2 3 5 7 11 13 17 19
        {3,   {0, 1, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0}},
        // 0 on positions of prime numbers (with leading 0): 1 2 3 5 7 11 13 17 19
        {3,   {1, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1}},
    };

    auto result = ColumnUInt8::create();
    auto & result_data = result->getData();

    while(result_data.size() < size)
    {
        for (const auto & [repeat, seq] : sequences)
        {
            for (size_t r = 0; r < repeat && result_data.size() < size; ++r)
            {
                const size_t to_insert = std::min<size_t>(seq.end() - seq.begin(), size - result_data.size());
                result_data.insert(seq.begin(), seq.begin() + to_insert);
            }
        }
    }

    MergeTreeDataPartDeletedMask mask;
    mask.deleted_rows = ColumnUInt8::Ptr(std::move(result));
    return mask;
}

template <UInt8 value>
MergeTreeDataPartDeletedMask generateDeletedRowsValues(size_t size)
{
    auto result = ColumnUInt8::create();
    auto & result_data = result->getData();
    result_data.resize_fill(size, value);

    MergeTreeDataPartDeletedMask mask;
    mask.deleted_rows = ColumnUInt8::Ptr(std::move(result));
    return mask;
}

class DeletedRowsTest : public ::testing::TestWithParam<std::tuple<size_t, GeneratorFunc>>
{
public:
    void SetUp() override
    {
        const auto & [size, generator] = GetParam();
        mask = generator(size);
    }
    MergeTreeDataPartDeletedMask mask;
};

TEST_P(DeletedRowsTest, SerializeAndDeserialize)
{
    std::vector<uint8_t> serialized_data;

    {
        WriteBufferFromVector write_buffer(serialized_data);
        mask.write(write_buffer);
        write_buffer.finalize();
    }

    std::cerr << "Input data size: " << mask.deleted_rows->size()
              << " serialized size: " << serialized_data.size() << std::endl;
    ASSERT_LT(0, serialized_data.size());
//    DUMP("!!!!! ", *mask.deleted_rows, serialized_data.size(), serialized_data);

    {
        ReadBufferFromMemory read_buffer(serialized_data.data(), serialized_data.size());
        MergeTreeDataPartDeletedMask loaded_mask;
        loaded_mask.read(read_buffer);

        ASSERT_EQ(mask, loaded_mask);
    }
}

INSTANTIATE_TEST_SUITE_P(MixedValues,
    DeletedRowsTest,
    ::testing::ValuesIn<DeletedRowsTest::ParamType>({
        {100,   &generateDeletedRows},
        {1000,  &generateDeletedRows},
        {10000, &generateDeletedRows},
        {1'000'000, &generateDeletedRows}
    })
);

INSTANTIATE_TEST_SUITE_P(Zeroes,
    DeletedRowsTest,
    ::testing::ValuesIn<DeletedRowsTest::ParamType>({
        {100,   &generateDeletedRowsValues<0>},
        {1000,  &generateDeletedRowsValues<0>},
        {10000, &generateDeletedRowsValues<0>},
        {1'000'000, &generateDeletedRowsValues<0>}
    })
);

INSTANTIATE_TEST_SUITE_P(Ones,
    DeletedRowsTest,
    ::testing::ValuesIn<DeletedRowsTest::ParamType>({
        {100,   &generateDeletedRowsValues<1>},
        {1000,  &generateDeletedRowsValues<1>},
        {10000, &generateDeletedRowsValues<1>},
        {1'000'000, &generateDeletedRowsValues<1>}
    })
);
