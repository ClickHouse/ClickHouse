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
}

TEST(QBitSerialization, FieldBinarySerializationFloat32)
{
    auto float32_type = DataTypeFactory::instance().get("Float32");
    auto qbit_type_float32 = std::static_pointer_cast<DataTypeQBit>(std::make_shared<DataTypeQBit>(float32_type, 5));
    auto serialization_float32 = qbit_type_float32->getDefaultSerialization();

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
    EXPECT_THROW_ERROR_CODE(std::make_shared<DataTypeQBit>(int32_type, 5), Exception, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}
