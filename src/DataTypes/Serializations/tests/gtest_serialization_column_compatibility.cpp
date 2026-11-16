#include <gtest/gtest.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationQBit.h>
#include <DataTypes/Serializations/SerializationSubObject.h>
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>

using namespace DB;

namespace
{

ColumnPtr nullableString()
{
    return ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
}

/// Serializations that deserialize into a column of a specific class reach into it with
/// `assert_cast`; `isColumnCompatible` is what MergeTree readers check result columns with.
TEST(SerializationIsColumnCompatible, StringRequiresColumnString)
{
    const auto versions = {MergeTreeStringSerializationVersion::SINGLE_STREAM, MergeTreeStringSerializationVersion::WITH_SIZE_STREAM};
    for (auto version : versions)
    {
        const auto serialization = SerializationString::create(version);

        EXPECT_TRUE(serialization->isColumnCompatible(*ColumnString::create()));
        EXPECT_FALSE(serialization->isColumnCompatible(*ColumnSparse::create(ColumnString::create())));
        EXPECT_FALSE(serialization->isColumnCompatible(*ColumnUInt8::create()));
        EXPECT_FALSE(serialization->isColumnCompatible(*nullableString()));
    }
}

TEST(SerializationIsColumnCompatible, SparseRequiresColumnSparse)
{
    const auto serialization = SerializationSparse::create(SerializationString::create());

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnSparse::create(ColumnString::create())));
    /// The nested type's own column is exactly the wrong one.
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, NullableRequiresColumnNullable)
{
    const auto serialization = SerializationNullable::create(SerializationString::create());

    EXPECT_TRUE(serialization->isColumnCompatible(*nullableString()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, SparseNullMapRequiresColumnUInt8)
{
    const auto serialization = SerializationSparseNullMap::create();
    ASSERT_TRUE(typeid_cast<const SerializationSparseNullMap *>(serialization.get()));

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnUInt8::create()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
    EXPECT_FALSE(serialization->isColumnCompatible(*nullableString()));
}

TEST(SerializationIsColumnCompatible, VariantElementNullMapRequiresColumnUInt8)
{
    const auto serialization = SerializationVariantElementNullMap::create("UInt64", 0, 2);

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnUInt8::create()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, DynamicRequiresColumnDynamic)
{
    const auto serialization = SerializationDynamic::create();

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnDynamic::create()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, VariantRequiresColumnVariant)
{
    const auto type = DataTypeFactory::instance().get("Variant(UInt32, String)");
    const auto serialization = type->getDefaultSerialization();
    ASSERT_TRUE(dynamic_cast<const SerializationVariant *>(serialization.get()) != nullptr);

    MutableColumns variants;
    variants.push_back(ColumnUInt32::create());
    variants.push_back(ColumnString::create());
    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnVariant::create(std::move(variants))));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, WrapperForwardsToNested)
{
    /// Subcolumn serializations are wrapped; a wrapper must keep the nested requirement,
    /// as its bulk forwards the same column to the nested serialization.
    const auto nested = SerializationSparse::create(SerializationString::create());
    const auto serialization = SerializationNamed::create(nested, "x", ISerialization::Substream::TupleElement);

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnSparse::create(ColumnString::create())));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, ObjectRequiresColumnObject)
{
    const auto type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32)");
    const auto serialization = type->getDefaultSerialization();
    /// The type hands out `SerializationJSON<...>`; the requirement is the one of its base.
    ASSERT_TRUE(dynamic_cast<const SerializationObject *>(serialization.get()) != nullptr);

    EXPECT_TRUE(serialization->isColumnCompatible(*type->createColumn()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

/// A `v.JSON` subcolumn reads into a Nullable(JSON) column: the element serialization passes
/// only the nested column to SerializationObject.
TEST(SerializationIsColumnCompatible, VariantElementUnwrapsExtractionNullable)
{
    const auto json_type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32)");
    const auto nullable_json_type = DataTypeFactory::instance().get("Nullable(JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32))");
    const auto serialization = SerializationVariantElement::create(
        json_type->getDefaultSerialization(), "JSON", /*variant_discriminator=*/0, /*num_variants=*/2,
        /*nullable_added_by_extraction=*/true);

    /// The column of the subcolumn type and the serialization-created one are both ColumnNullable.
    EXPECT_TRUE(serialization->isColumnCompatible(*nullable_json_type->createColumn()));
    EXPECT_TRUE(serialization->isColumnCompatible(*nullable_json_type->createColumn(*serialization)));
    /// Forwarding the outer column to the nested requirement would reject the columns above.
    EXPECT_FALSE(serialization->isColumnCompatible(*nullableString()));
}

TEST(SerializationIsColumnCompatible, VariantElementSelectedNullMapRequiresColumnUInt8)
{
    const auto serialization = SerializationVariantElement::create(
        SerializationString::create(), "Tuple(a Nullable(UInt32))", /*variant_discriminator=*/0, /*num_variants=*/1,
        /*nullable_added_by_extraction=*/false, /*selected_subcolumn_is_null_map=*/true);

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnUInt8::create()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

/// A `d.JSON` subcolumn of Dynamic delegates to SerializationVariantElement with the same column.
TEST(SerializationIsColumnCompatible, DynamicElementUnwrapsExtractionNullable)
{
    const auto json_type = DataTypeFactory::instance().get("JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32)");
    const auto nullable_json_type = DataTypeFactory::instance().get("Nullable(JSON(max_dynamic_types=10, max_dynamic_paths=2, a.b UInt32))");
    const auto serialization = SerializationDynamicElement::create(
        json_type->getDefaultSerialization(), SerializationString::create(), "JSON", /*nested_subcolumn=*/"",
        /*is_null_map_subcolumn=*/false, /*nullable_added_by_extraction=*/true);

    EXPECT_TRUE(serialization->isColumnCompatible(*nullable_json_type->createColumn()));
    EXPECT_TRUE(serialization->isColumnCompatible(*nullable_json_type->createColumn(*serialization)));
    EXPECT_FALSE(serialization->isColumnCompatible(*nullableString()));
}

TEST(SerializationIsColumnCompatible, DynamicElementNullMapRequiresColumnUInt8)
{
    const auto serialization = SerializationDynamicElement::create(
        SerializationString::create(), SerializationString::create(), "Int64", /*nested_subcolumn=*/"",
        /*is_null_map_subcolumn=*/true, /*nullable_added_by_extraction=*/false);

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnUInt8::create()));
    EXPECT_FALSE(serialization->isColumnCompatible(*nullableString()));
}

/// A plain tuple of the type's columns would reach SerializationSparse's unchecked cast
/// when the element is stored sparsely.
TEST(SerializationIsColumnCompatible, MapRequiresColumnMap)
{
    const auto type = DataTypeFactory::instance().get("Map(String, UInt64)");
    const auto serialization = type->getDefaultSerialization();
    ASSERT_TRUE(dynamic_cast<const SerializationMap *>(serialization.get()) != nullptr);

    EXPECT_TRUE(serialization->isColumnCompatible(*type->createColumn()));
    /// A constant column of the same SQL type is still the wrong destination.
    auto default_column = type->createColumn();
    type->insertDefaultInto(*default_column);
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnConst::create(std::move(default_column), 1)));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, QBitRequiresColumnQBit)
{
    const auto type = DataTypeFactory::instance().get("QBit(Float32, 2)");
    const auto serialization = type->getDefaultSerialization();
    ASSERT_TRUE(dynamic_cast<const SerializationQBit *>(serialization.get()) != nullptr);

    EXPECT_TRUE(serialization->isColumnCompatible(*type->createColumn()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, SubObjectRequiresColumnObject)
{
    const auto dynamic_type = DataTypeFactory::instance().get("Dynamic");
    const auto serialization = SerializationSubObject::create(
        "a", {}, dynamic_type, dynamic_type->getDefaultSerialization(), nullptr);

    const auto json_type = DataTypeFactory::instance().get("JSON(a.b UInt32)");
    EXPECT_TRUE(serialization->isColumnCompatible(*json_type->createColumn()));
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
}

TEST(SerializationIsColumnCompatible, TupleChecksNestedElements)
{
    SerializationTuple::ElementSerializations element_serializations;
    element_serializations.push_back(std::static_pointer_cast<const SerializationNamed>(
        SerializationNamed::create(SerializationSparse::create(SerializationString::create()), "s", ISerialization::Substream::TupleElement)));
    element_serializations.push_back(std::static_pointer_cast<const SerializationNamed>(
        SerializationNamed::create(SerializationString::create(), "t", ISerialization::Substream::TupleElement)));
    const auto serialization = SerializationTuple::create(
        std::move(element_serializations),
        DataTypes{DataTypeFactory::instance().get("String"), DataTypeFactory::instance().get("String")},
        /*has_explicit_names=*/true);

    const auto plain_tuple = ColumnTuple::create(Columns{ColumnString::create(), ColumnString::create()});
    EXPECT_FALSE(serialization->isColumnCompatible(*plain_tuple));

    /// The tuple the readers create through the serialization is accepted.
    const auto wrapped_tuple = serialization->wrapColumnForDeserialization(plain_tuple->cloneEmpty());
    EXPECT_TRUE(serialization->isColumnCompatible(*wrapped_tuple));

    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnString::create()));
    /// A tuple of a wrong width is a mistake of its own.
    EXPECT_FALSE(serialization->isColumnCompatible(*ColumnTuple::create(Columns{ColumnString::create()})));
}

}
