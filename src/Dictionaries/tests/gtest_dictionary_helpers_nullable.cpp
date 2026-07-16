#include <gtest/gtest.h>

#include <Dictionaries/DictionaryHelpers.h>
#include <Dictionaries/DictionaryStructure.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnObject.h>
#include <Common/typeid_cast.h>

using namespace DB;

namespace
{

// Note: Map type cannot be inside Nullable in ClickHouse.
// This function is kept for reference but won't be used in tests
// because makeNullable(Map(...)) will throw an exception.

DictionaryAttribute makeNullableJsonAttribute()
{
    // Nullable(JSON) - JSON is syntactic sugar for Object(JSON)
    DataTypePtr json_type = std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
    DataTypePtr nullable_json_type = makeNullable(json_type);

    return DictionaryAttribute{
        .name = "j",
        .type = nullable_json_type,
        .type_serialization = nullable_json_type->getDefaultSerialization(),
        .expression = "",
        .null_value = Field{},
        .underlying_type = AttributeUnderlyingType::Object,
        .hierarchical = false,
        .bidirectional = false,
        .injective = false,
        .is_object_id = false,
        .is_nullable = true
    };
}

} // namespace

TEST(DictionaryAttributeColumnProvider, MapTypeSupported)
{
    // Note: Map type cannot be inside Nullable in ClickHouse (canBeInsideNullable returns false).
    // This test verifies that getColumn correctly handles a Map type (non-nullable).
    // The fix we made ensures that removeNullable is used to unwrap any Nullable wrapper,
    // even though Map cannot be Nullable in normal cases.

    // Create a non-nullable Map attribute
    DataTypePtr key_type = std::make_shared<DataTypeString>();
    DataTypePtr value_type = std::make_shared<DataTypeString>();
    DataTypePtr map_type = std::make_shared<DataTypeMap>(key_type, value_type);

    DictionaryAttribute attr{
        .name = "m",
        .type = map_type,
        .type_serialization = map_type->getDefaultSerialization(),
        .expression = "",
        .null_value = Field{},
        .underlying_type = AttributeUnderlyingType::Map,
        .hierarchical = false,
        .bidirectional = false,
        .injective = false,
        .is_object_id = false,
        .is_nullable = false
    };

    EXPECT_NO_THROW({
        auto col = DictionaryAttributeColumnProvider<Map>::getColumn(attr, /*size*/ 0);
        // Should get ColumnMap
        auto * map_col = typeid_cast<ColumnMap *>(col.get());
        EXPECT_NE(map_col, nullptr);
    });

    // Verify that removeNullable correctly handles non-nullable types
    DataTypePtr non_nullable = removeNullable(map_type);
    EXPECT_EQ(non_nullable.get(), map_type.get());
}

TEST(DictionaryAttributeColumnProvider, NullableJsonTypeSupported)
{
    auto attr = makeNullableJsonAttribute();

    EXPECT_NO_THROW({
        auto col = DictionaryAttributeColumnProvider<Object>::getColumn(attr, /*size*/ 0);
        // Should get ColumnObject
        auto * obj_col = typeid_cast<ColumnObject *>(col.get());
        EXPECT_NE(obj_col, nullptr);
    });
}

TEST(DictionaryAttributeColumnProvider, TypedJsonPathsPreservedForSubcolumns)
{
    std::unordered_map<String, DataTypePtr> typed_paths; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    typed_paths.emplace("name", std::make_shared<DataTypeString>());
    typed_paths.emplace("age", std::make_shared<DataTypeUInt32>());

    DataTypePtr json_type = std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        typed_paths,
        std::unordered_set<String>{}, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<String>{},
        DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS,
        DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES);
    DataTypePtr nullable_json_type = makeNullable(json_type);

    DictionaryAttribute attr{
        .name = "typed_j",
        .type = nullable_json_type,
        .type_serialization = nullable_json_type->getDefaultSerialization(),
        .expression = "",
        .null_value = Field{},
        .underlying_type = AttributeUnderlyingType::Object,
        .hierarchical = false,
        .bidirectional = false,
        .injective = false,
        .is_object_id = false,
        .is_nullable = true
    };

    auto col = DictionaryAttributeColumnProvider<Object>::getColumn(attr, /*size*/ 0);
    auto * obj_col = typeid_cast<ColumnObject *>(col.get());
    ASSERT_NE(obj_col, nullptr);
    EXPECT_EQ(obj_col->getTypedPaths().size(), 2);

    /// Reproduces previous failure path for `dictGet(...).name`:
    /// subcolumn resolution on empty dictionary column should not throw.
    auto non_nullable_type = removeNullable(attr.type);
    EXPECT_NO_THROW({
        ColumnPtr immutable_col = col->getPtr();
        auto subcolumn = non_nullable_type->getSubcolumn("name", immutable_col);
        EXPECT_NE(subcolumn, nullptr);
    });
}
