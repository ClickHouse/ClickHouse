#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <gtest/gtest.h>

using namespace DB;

GTEST_TEST(NestedUtils, collect)
{
    DataTypePtr uint_type = std::make_shared<DataTypeUInt32>();
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());

    const NamesAndTypesList source_columns =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b.arr1", array_type},
        {"b.arr2", array_type}
    };

    auto nested_type = createNested({uint_type, uint_type}, {"arr1", "arr2"});
    const NamesAndTypesList columns_with_subcolumns =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b", "arr1", nested_type, array_type},
        {"b", "arr2", nested_type, array_type}
    };

    const NamesAndTypesList columns_with_nested =
    {
        {"id", uint_type},
        {"arr1", array_type},
        {"b.id", uint_type},
        {"b", nested_type},
    };

    ASSERT_EQ(Nested::convertToSubcolumns(source_columns).toString(), columns_with_subcolumns.toString());
    ASSERT_EQ(Nested::collect(source_columns).toString(), columns_with_nested.toString());
}

/// A subcolumn entry's type in storage is the type in metadata, while a plain entry carries the type
/// its caller resolved -- for a MergeTree part being read, the part's own possibly older type. The
/// group's element type must come from the plain entry in either order, or a type-directed
/// `enumerateStreams` walk over these columns casts the part's column to the metadata's class.
GTEST_TEST(NestedUtils, convertToSubcolumnsPrefersColumnOverSubcolumn)
{
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr array_of_string = std::make_shared<DataTypeArray>(string_type);
    DataTypePtr array_of_nullable_string = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(string_type));
    DataTypePtr array_of_uint8 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());

    /// `b.n` is present in the part as Array(String), while metadata says Array(Nullable(String)).
    const NameAndTypePair plain_member{"b.n", array_of_string};
    const NameAndTypePair null_subcolumn{"b.n", "null", array_of_nullable_string, array_of_uint8};

    auto element_type_of = [](const NamesAndTypesList & input)
    {
        for (const auto & name_type : Nested::convertToSubcolumns(input))
        {
            if (name_type.getNameInStorage() == "b" && name_type.getSubcolumnName() == "n")
                return name_type.type;
        }
        return DataTypePtr{};
    };

    ASSERT_EQ(element_type_of({null_subcolumn, plain_member})->getName(), array_of_string->getName());
    ASSERT_EQ(element_type_of({plain_member, null_subcolumn})->getName(), array_of_string->getName());

    /// A member requested only as a subcolumn still contributes and is still remapped onto the Nested
    /// type -- that is what makes the shared offsets serialization apply to it.
    bool remapped_onto_nested = false;
    for (const auto & name_type : Nested::convertToSubcolumns({{"b.i", array_of_uint8}, null_subcolumn}))
    {
        if (name_type.getSubcolumnName() == "n.null")
            remapped_onto_nested = isNested(name_type.getTypeInStorage());
    }
    ASSERT_TRUE(remapped_onto_nested);
}
