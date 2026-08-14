#include <gtest/gtest.h>

#include <Storages/ObjectStorage/Utils.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

using namespace DB;

namespace
{

/// Builds the metadata a lake read reaches `dropSortingKeyIfItDoesNotDescribeColumns` with:
/// the sorting key is resolved against the lake's own schema, exactly as
/// `getSortingKeyDescriptionFromMetadata` does, and `columns` is then whatever the read will
/// actually emit (the lake schema, or a narrower/retyped structure the caller declared).
StorageInMemoryMetadata makeMetadata(
    const NamesAndTypesList & lake_schema, const String & order_by, const NamesAndTypesList & emitted_columns)
{
    tryRegisterFunctions();

    StorageInMemoryMetadata metadata;
    metadata.sorting_key = KeyDescription::parse(order_by, ColumnsDescription(lake_schema), {}, getContext().context, true);
    metadata.setColumns(ColumnsDescription(emitted_columns));
    return metadata;
}

NameAndTypePair column(const String & name, const String & type)
{
    return {name, DataTypeFactory::instance().get(type)};
}

}

/// A nested key component arrives as the dotted subcolumn name `st.a`, which lives in the
/// subcolumns index rather than the top-level one.
TEST(DropSortingKey, KeepsNestedKeyDescribingTheColumns)
{
    auto metadata = makeMetadata({column("st", "Tuple(a Int64)")}, "`st.a` ASC", {column("st", "Tuple(a Int64)")});
    ASSERT_TRUE(metadata.hasSortingKey());

    dropSortingKeyIfItDoesNotDescribeColumns(metadata);

    EXPECT_TRUE(metadata.hasSortingKey());
    EXPECT_EQ(metadata.getSortingKeyColumns(), Names{"st.a"});
}

TEST(DropSortingKey, DropsNestedKeyWhenItsColumnIsNotEmitted)
{
    auto metadata = makeMetadata({column("st", "Tuple(a Int64)")}, "`st.a` ASC", {column("id", "Int64")});
    ASSERT_TRUE(metadata.hasSortingKey());

    dropSortingKeyIfItDoesNotDescribeColumns(metadata);

    EXPECT_FALSE(metadata.hasSortingKey());
}

TEST(DropSortingKey, DropsNestedKeyWhenItsColumnIsRetyped)
{
    auto metadata = makeMetadata({column("st", "Tuple(a Int64)")}, "`st.a` ASC", {column("st", "Tuple(a String)")});
    ASSERT_TRUE(metadata.hasSortingKey());

    dropSortingKeyIfItDoesNotDescribeColumns(metadata);

    EXPECT_FALSE(metadata.hasSortingKey());
}

TEST(DropSortingKey, KeepsTopLevelKeyDescribingTheColumns)
{
    auto metadata = makeMetadata({column("id", "Int64")}, "id ASC", {column("id", "Int64")});
    ASSERT_TRUE(metadata.hasSortingKey());

    dropSortingKeyIfItDoesNotDescribeColumns(metadata);

    EXPECT_TRUE(metadata.hasSortingKey());
    EXPECT_EQ(metadata.getSortingKeyColumns(), Names{"id"});
}
