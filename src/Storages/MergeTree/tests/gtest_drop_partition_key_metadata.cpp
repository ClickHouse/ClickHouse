#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>

#include <array>

namespace DB
{
namespace ErrorCodes
{
    extern const int METADATA_MISMATCH;
}

TEST(ReplicatedMergeTreeTableMetadata, OnlyAllowDroppingPartitionKey)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();
    const auto & context = ::getContext().context;
    const auto type = std::make_shared<DataTypeUInt64>();
    const ColumnsDescription columns(NamesAndTypesList{{"x", type}, {"y", type}});
    const VirtualColumnsDescription virtuals;

    struct TestCase
    {
        const char * local_key;
        const char * keeper_key;
        bool reject;
        bool changed;
    };

    const std::array cases = {
        TestCase{"x", "x", false, false},
        TestCase{"x", "(x)", false, false},
        TestCase{"x, y", "x, y", false, false},
        TestCase{"x", "y", true, false},
        TestCase{"x, y", "y, x", true, false},
        TestCase{"", "x", true, false},
        TestCase{"x", "", false, true},
        TestCase{"", "", false, false},
    };

    auto expect_metadata_mismatch = [](auto && check)
    {
        try
        {
            check();
            FAIL() << "Expected METADATA_MISMATCH";
        }
        catch (const Exception & error)
        {
            EXPECT_EQ(error.code(), ErrorCodes::METADATA_MISMATCH);
        }
    };

    for (const auto & test_case : cases)
    {
        SCOPED_TRACE(String(test_case.local_key) + " -> " + test_case.keeper_key);
        ReplicatedMergeTreeTableMetadata local;
        local.data_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
        local.primary_key = "x";
        local.partition_key = test_case.local_key;
        auto keeper = local;
        keeper.partition_key = test_case.keeper_key;

        auto check_equals = [&] { return local.checkEquals(keeper, columns, virtuals, "test", context, true, false); };
        auto find_diff = [&] { return local.checkAndFindDiff(keeper, columns, virtuals, "test", context); };
        if (test_case.reject)
        {
            expect_metadata_mismatch(check_equals);
            expect_metadata_mismatch(find_diff);
        }
        else
        {
            EXPECT_EQ(check_equals(), !test_case.changed);
            const auto diff = find_diff();
            EXPECT_EQ(diff.partition_key_changed, test_case.changed);
            EXPECT_EQ(diff.empty(), !test_case.changed);
            if (test_case.changed)
                EXPECT_TRUE(diff.new_partition_key.empty());
        }
    }
}
}
