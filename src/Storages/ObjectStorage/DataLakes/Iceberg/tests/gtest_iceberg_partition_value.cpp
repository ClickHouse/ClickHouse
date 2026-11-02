#include <gtest/gtest.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#if USE_AVRO

#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>

using namespace DB;

/// A `Bool` partition value decodes to `Int64` from an Avro `int` and to `UInt64` from an Avro `boolean`.
TEST(IcebergPartitionValue, BoolCarrierIsCanonical)
{
    for (const auto * type_name : {"Bool", "Nullable(Bool)"})
    {
        SCOPED_TRACE(type_name);
        const auto type = DataTypeFactory::instance().get(type_name);

        for (Int64 x : {0, 1})
        {
            const Field from_int = Iceberg::normalizePartitionValue(Field(x), type);
            const Field from_boolean = Iceberg::normalizePartitionValue(Field(static_cast<UInt64>(x)), type);
            EXPECT_EQ(from_int.getType(), Field::Types::UInt64);
            EXPECT_EQ(from_boolean.getType(), Field::Types::UInt64);
            EXPECT_TRUE(from_int == from_boolean);
            EXPECT_EQ(from_int.safeGet<UInt64>(), static_cast<UInt64>(x));
        }

        EXPECT_TRUE(Iceberg::normalizePartitionValue(Field(), type).isNull());
    }

    const Field int_value = Iceberg::normalizePartitionValue(Field(Int64(1)), DataTypeFactory::instance().get("Int32"));
    EXPECT_EQ(int_value.getType(), Field::Types::Int64);
    EXPECT_EQ(int_value.safeGet<Int64>(), 1);
}

#endif
