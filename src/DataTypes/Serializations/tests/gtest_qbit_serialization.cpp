#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeQBit.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/ErrorCodes.h>

#include <base/types.h>
#include <gtest/gtest.h>


#define EXPECT_THROW_ERROR_CODE(statement, expected_exception, expected_code) \
    EXPECT_THROW( \
        try { statement; } catch (const expected_exception & e) { \
            EXPECT_EQ(expected_code, e.code()); \
            throw; \
        }, \
        expected_exception)


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int UNEXPECTED_AST_STRUCTURE;
}

TEST(QBitSerialization, FieldBinarySerializationFloat32)
{
    auto float32_type = DataTypeFactory::instance().get("Float32");
    DataTypeQBit qbit_type_float32(float32_type, 5, 5);
    auto serialization_float32 = qbit_type_float32.getDefaultSerialization();

    const size_t bytes_per_fixedstring = 1;

    Tuple tuple_elements;
    tuple_elements.reserve(32); /// 32 bits for Float32

    for (size_t i = 0; i < 32; ++i)
    {
        String fixed_string(bytes_per_fixedstring, static_cast<char>(i));
        tuple_elements.push_back(Field(std::move(fixed_string)));
    }

    Field test_field = tuple_elements;

    /// Serialize and deserialize
    String serialized_data;
    WriteBufferFromString write_buffer(serialized_data);
    FormatSettings format_settings;
    serialization_float32->serializeBinary(test_field, write_buffer, format_settings);

    ReadBufferFromString read_buffer(serialized_data);
    Field deserialized_field;
    serialization_float32->deserializeBinary(deserialized_field, read_buffer, format_settings);

    /// Verify the result
    ASSERT_EQ(deserialized_field.getType(), Field::Types::Tuple);
    const Tuple & result_tuple = deserialized_field.safeGet<Tuple>();
    ASSERT_EQ(result_tuple.size(), 32);

    for (size_t i = 0; i < 32; ++i)
    {
        ASSERT_EQ(result_tuple[i].getType(), Field::Types::String);
        const String & fixed_string = result_tuple[i].safeGet<String>();
        ASSERT_EQ(fixed_string.size(), bytes_per_fixedstring);
        ASSERT_EQ(fixed_string[0], static_cast<char>(i));
    }
}

TEST(QBitSerialization, RejectInvalidElementType)
{
    auto int32_type = DataTypeFactory::instance().get("Int32");
    EXPECT_THROW_ERROR_CODE(DataTypeQBit(int32_type, 5, 5), Exception, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

TEST(QBitSerialization, RejectInvalidStride)
{
    auto float32_type = DataTypeFactory::instance().get("Float32");

    /// Stride must be a positive divisor of the dimension.
    EXPECT_THROW_ERROR_CODE(DataTypeQBit(float32_type, 8, 0), Exception, ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    EXPECT_THROW_ERROR_CODE(DataTypeQBit(float32_type, 8, 16), Exception, ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    EXPECT_THROW_ERROR_CODE(DataTypeQBit(float32_type, 8, 3), Exception, ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    /// When strided (stride != dimension), the stride must be a multiple of 8.
    EXPECT_THROW_ERROR_CODE(DataTypeQBit(float32_type, 12, 4), Exception, ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    EXPECT_THROW_ERROR_CODE(DataTypeQBit(float32_type, 32, 12), Exception, ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    /// Valid: non-strided (any dimension) and strided with stride a multiple of 8 dividing the dimension.
    EXPECT_NO_THROW(DataTypeQBit(float32_type, 5, 5));
    EXPECT_NO_THROW(DataTypeQBit(float32_type, 16, 8));

    /// The MAX_STRIDE_GROUPS bound is exercised end-to-end in 04497_qbit_stride_max_groups.sql.
}

TEST(QBitSerialization, FieldBinarySerializationStridedFloat32)
{
    auto float32_type = DataTypeFactory::instance().get("Float32");
    /// 16 dimensions split into two stride groups of 8 (no padding: 8 bits == 1 byte per group's bit plane).
    DataTypeQBit qbit_type(float32_type, 16, 8);
    auto serialization = qbit_type.getDefaultSerialization();

    /// element_size (32) * num_strides (2) = 64 bit-plane FixedString columns, each 1 byte.
    const size_t num_columns = 64;
    const size_t bytes_per_fixedstring = 1;

    Tuple tuple_elements;
    tuple_elements.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        tuple_elements.push_back(Field(String(bytes_per_fixedstring, static_cast<char>(i))));

    Field test_field = tuple_elements;

    String serialized_data;
    WriteBufferFromString write_buffer(serialized_data);
    FormatSettings format_settings;
    serialization->serializeBinary(test_field, write_buffer, format_settings);

    ReadBufferFromString read_buffer(serialized_data);
    Field deserialized_field;
    serialization->deserializeBinary(deserialized_field, read_buffer, format_settings);

    ASSERT_EQ(deserialized_field.getType(), Field::Types::Tuple);
    const Tuple & result_tuple = deserialized_field.safeGet<Tuple>();
    ASSERT_EQ(result_tuple.size(), num_columns);

    /// Stride is a multiple of 8, so every bit is a real dimension and the tuple -> floats -> tuple round trip is the identity.
    for (size_t i = 0; i < num_columns; ++i)
    {
        ASSERT_EQ(result_tuple[i].getType(), Field::Types::String);
        const String & fixed_string = result_tuple[i].safeGet<String>();
        ASSERT_EQ(fixed_string.size(), bytes_per_fixedstring);
        ASSERT_EQ(fixed_string[0], static_cast<char>(i));
    }
}

}
