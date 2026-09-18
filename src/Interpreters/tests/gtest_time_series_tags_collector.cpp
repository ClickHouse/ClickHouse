#include <gtest/gtest.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Common/Exception.h>

#include <array>
#include <string_view>
#include <thread>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
extern const int LOGICAL_ERROR;
}

namespace
{

using Tags = ContextTimeSeriesTagsCollector::TagNamesAndValues;
using TagsPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
using TagsVector = VectorWithMemoryTracking<TagsPtr>;

TagsPtr makeTags(std::initializer_list<std::pair<String, String>> values)
{
    auto tags = std::make_shared<Tags>();
    tags->assign(values.begin(), values.end());
    return tags;
}

template <typename Function>
void expectExceptionCode(Function && function, int expected_code)
{
    try
    {
        function();
        FAIL() << "Expected DB::Exception with code " << expected_code;
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << exception.message();
    }
}

ColumnPtr makeUInt64IDs(std::initializer_list<UInt64> values)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value : values)
        column->insertValue(value);
    return column;
}

ColumnPtr makeUInt128IDs(std::initializer_list<UInt64> values)
{
    auto column = ColumnUInt128::create();
    for (UInt64 value : values)
        column->insertValue(UInt128{value});
    return column;
}

ColumnPtr makeStringIDs(std::initializer_list<std::string_view> values)
{
    auto column = ColumnString::create();
    for (std::string_view value : values)
        column->insertData(value.data(), value.size());
    return column;
}

void expectUnknownID(ContextTimeSeriesTagsCollector & collector, const ColumnPtr & ids)
{
    expectExceptionCode(
        [&]
        {
            PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
            collector.getGroupByID(ids, groups);
        },
        ErrorCodes::BAD_ARGUMENTS);
}

}


TEST(ContextTimeSeriesTagsCollector, StandardStoreAllowsDifferentIDsWithSameTags)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}, {"job", "test"}});

    collector.storeTags(makeUInt64IDs({1, 2}), TagsVector{tags, tags});

    PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
    collector.getGroupByID(makeUInt64IDs({1, 2}), groups);
    ASSERT_EQ(groups.size(), 2);
    EXPECT_EQ(groups[0], groups[1]);
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryAllowsRepeatedIDAndSeals)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});

    collector.startNativeSeriesDictionaryBuild();
    collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({1, 1}), TagsVector{tags, tags});
    collector.finishNativeSeriesDictionaryBuild();

    EXPECT_TRUE(collector.isNativeSeriesDictionaryBuilt());
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryRejectsSameIDWithDifferentTags)
{
    ContextTimeSeriesTagsCollector collector;
    auto first_tags = makeTags({{"__name__", "up"}, {"job", "first"}});
    auto second_tags = makeTags({{"__name__", "up"}, {"job", "second"}});

    collector.startNativeSeriesDictionaryBuild();
    expectExceptionCode(
        [&] { collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({1, 1}), TagsVector{first_tags, second_tags}); },
        ErrorCodes::BAD_ARGUMENTS);

    EXPECT_FALSE(collector.isNativeSeriesDictionaryBuilt());
    expectUnknownID(collector, makeUInt64IDs({1}));
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryRejectsDifferentIDsWithSameTags)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});

    collector.startNativeSeriesDictionaryBuild();
    expectExceptionCode(
        [&] { collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({1, 2}), TagsVector{tags, tags}); },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);

    EXPECT_FALSE(collector.isNativeSeriesDictionaryBuilt());
    expectUnknownID(collector, makeUInt64IDs({1}));
    expectUnknownID(collector, makeUInt64IDs({2}));
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryRejectsGenericDifferentIDsWithSameTags)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});
    auto ids = ColumnString::create();
    ids->insertData("first", 5);
    ids->insertData("second", 6);

    collector.startNativeSeriesDictionaryBuild();
    expectExceptionCode(
        [&] { collector.storeTagsForNativeSeriesDictionary(std::move(ids), TagsVector{tags, tags}); },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
    auto first_id = ColumnString::create();
    first_id->insertData("first", 5);
    expectUnknownID(collector, std::move(first_id));
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryRollbackPreservesPreviousChunks)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});

    collector.startNativeSeriesDictionaryBuild();
    collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({1}), TagsVector{tags});
    expectExceptionCode(
        [&] { collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({2}), TagsVector{tags}); },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);

    PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
    collector.getGroupByID(makeUInt64IDs({1}), groups);
    ASSERT_EQ(groups.size(), 1);
    expectUnknownID(collector, makeUInt64IDs({2}));
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryRejectsDuplicateTagsAcrossIDMaps)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});

    collector.startNativeSeriesDictionaryBuild();
    collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({1}), TagsVector{tags});
    expectExceptionCode(
        [&] { collector.storeTagsForNativeSeriesDictionary(makeUInt128IDs({1}), TagsVector{tags}); },
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);

    PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
    collector.getGroupByID(makeUInt64IDs({1}), groups);
    ASSERT_EQ(groups.size(), 1);
    expectUnknownID(collector, makeUInt128IDs({1}));
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionarySkipsNullableIDs)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});
    auto null_map = ColumnUInt8::create();
    null_map->insertValue(0);
    null_map->insertValue(1);
    auto ids = ColumnNullable::create(makeUInt64IDs({1, 2})->assumeMutable(), std::move(null_map));

    collector.startNativeSeriesDictionaryBuild();
    collector.storeTagsForNativeSeriesDictionary(std::move(ids), TagsVector{tags, tags});
    collector.finishNativeSeriesDictionaryBuild();

    EXPECT_TRUE(collector.isNativeSeriesDictionaryBuilt());
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryCanSealEmptyInput)
{
    ContextTimeSeriesTagsCollector collector;

    collector.startNativeSeriesDictionaryBuild();
    collector.finishNativeSeriesDictionaryBuild();

    EXPECT_TRUE(collector.isNativeSeriesDictionaryBuilt());
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionaryAbortPreventsSeal)
{
    ContextTimeSeriesTagsCollector collector;

    collector.startNativeSeriesDictionaryBuild();
    collector.abortNativeSeriesDictionaryBuild();
    collector.abortNativeSeriesDictionaryBuild();

    EXPECT_FALSE(collector.isNativeSeriesDictionaryBuilt());
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}


TEST(ContextTimeSeriesTagsCollector, StandardStoreCannotBypassNativeDictionaryValidation)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});

    collector.startNativeSeriesDictionaryBuild();
    expectExceptionCode([&] { collector.storeTags(makeUInt64IDs({1}), TagsVector{tags}); }, ErrorCodes::LOGICAL_ERROR);
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionarySerializesConcurrentDuplicateValidation)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});
    std::array<int, 2> result_codes{};

    collector.startNativeSeriesDictionaryBuild();
    std::thread first(
        [&]
        {
            try
            {
                collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({1}), TagsVector{tags});
            }
            catch (const Exception & exception)
            {
                result_codes[0] = exception.code();
            }
        });
    std::thread second(
        [&]
        {
            try
            {
                collector.storeTagsForNativeSeriesDictionary(makeUInt64IDs({2}), TagsVector{tags});
            }
            catch (const Exception & exception)
            {
                result_codes[1] = exception.code();
            }
        });
    first.join();
    second.join();

    const int successes = (result_codes[0] == 0) + (result_codes[1] == 0);
    const int duplicate_failures
        = (result_codes[0] == ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY) + (result_codes[1] == ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
    EXPECT_EQ(successes, 1);
    EXPECT_EQ(duplicate_failures, 1);
    if (result_codes[0] == 0)
    {
        PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
        collector.getGroupByID(makeUInt64IDs({1}), groups);
        expectUnknownID(collector, makeUInt64IDs({2}));
    }
    else
    {
        PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
        collector.getGroupByID(makeUInt64IDs({2}), groups);
        expectUnknownID(collector, makeUInt64IDs({1}));
    }
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}


TEST(ContextTimeSeriesTagsCollector, NativeDictionarySerializesConcurrentGenericDuplicateValidation)
{
    ContextTimeSeriesTagsCollector collector;
    auto tags = makeTags({{"__name__", "up"}});
    std::array<int, 2> result_codes{};

    collector.startNativeSeriesDictionaryBuild();
    std::thread first(
        [&]
        {
            try
            {
                collector.storeTagsForNativeSeriesDictionary(makeStringIDs({"first"}), TagsVector{tags});
            }
            catch (const Exception & exception)
            {
                result_codes[0] = exception.code();
            }
        });
    std::thread second(
        [&]
        {
            try
            {
                collector.storeTagsForNativeSeriesDictionary(makeStringIDs({"second"}), TagsVector{tags});
            }
            catch (const Exception & exception)
            {
                result_codes[1] = exception.code();
            }
        });
    first.join();
    second.join();

    const int successes = (result_codes[0] == 0) + (result_codes[1] == 0);
    const int duplicate_failures
        = (result_codes[0] == ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY) + (result_codes[1] == ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY);
    EXPECT_EQ(successes, 1);
    EXPECT_EQ(duplicate_failures, 1);
    if (result_codes[0] == 0)
    {
        PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
        collector.getGroupByID(makeStringIDs({"first"}), groups);
        expectUnknownID(collector, makeStringIDs({"second"}));
    }
    else
    {
        PaddedPODArray<ContextTimeSeriesTagsCollector::Group> groups;
        collector.getGroupByID(makeStringIDs({"second"}), groups);
        expectUnknownID(collector, makeStringIDs({"first"}));
    }
    expectExceptionCode([&] { collector.finishNativeSeriesDictionaryBuild(); }, ErrorCodes::LOGICAL_ERROR);
}

}
