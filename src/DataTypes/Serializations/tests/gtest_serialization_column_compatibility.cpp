#include <gtest/gtest.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationSparse.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/Serializations/SerializationVariant.h>
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
TEST(SerializationIsColumnCompatible, PlainSerializationAcceptsAnyColumn)
{
    const auto serialization = SerializationString::create();

    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnString::create()));
    /// The default declares no class requirement; a wrong column type is a different contract.
    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnUInt8::create()));
    EXPECT_TRUE(serialization->isColumnCompatible(*nullableString()));
    EXPECT_TRUE(serialization->isColumnCompatible(*ColumnSparse::create(ColumnString::create())));
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

}
