#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeQBit.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <base/types.h>
#include <gtest/gtest.h>


using namespace DB;

class QBitSerializationTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        auto float32_type = DataTypeFactory::instance().get("Float32");
        qbit_type_float32 = std::static_pointer_cast<DataTypeQBit>(std::make_shared<DataTypeQBit>(float32_type, 5));
        serialization_float32 = qbit_type_float32->getDefaultSerialization();
    }

    std::shared_ptr<DataTypeQBit> qbit_type_float32;
    SerializationPtr serialization_float32;
};

TEST_F(QBitSerializationTest, FieldBinarySerializationFloat32)
{
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
