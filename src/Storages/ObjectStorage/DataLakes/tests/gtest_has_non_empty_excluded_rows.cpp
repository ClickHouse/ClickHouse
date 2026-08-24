#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeObjectMetadata.h>

using namespace DB;

TEST(HasNonEmptyExcludedRows, EmptyOptionalIsFalse)
{
    EXPECT_FALSE(hasNonEmptyExcludedRows(std::nullopt));
}

TEST(HasNonEmptyExcludedRows, MissingOrEmptyBitmapIsFalse)
{
    DataLakeObjectMetadata metadata;
    EXPECT_FALSE(hasNonEmptyExcludedRows(metadata));

    metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    EXPECT_FALSE(hasNonEmptyExcludedRows(metadata));
}

TEST(HasNonEmptyExcludedRows, NonEmptyBitmapIsTrue)
{
    DataLakeObjectMetadata metadata;
    metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    metadata.excluded_rows->add(7);
    EXPECT_TRUE(hasNonEmptyExcludedRows(metadata));
}
