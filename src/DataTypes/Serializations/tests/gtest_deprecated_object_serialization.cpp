#include <Columns/ColumnObjectDeprecated.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/Serializations/SerializationObjectDeprecated.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorToString.h>

#include <gtest/gtest.h>

#if USE_SIMDJSON

using namespace DB;

TEST(SerializationObjectDeprecated, FromString)
{
    WriteBufferFromOwnString out;

    auto column_string = ColumnString::create();
    column_string->insert(R"({"k1" : 1, "k2" : [{"k3" : "aa", "k4" : 2}, {"k3": "bb", "k4": 3}]})");
    column_string->insert(R"({"k1" : 2, "k2" : [{"k3" : "cc", "k5" : 4}, {"k4": 5}, {"k4": 6}]})");

    {
        auto serialization = std::make_shared<SerializationString>();

        ISerialization::SerializeBinaryBulkSettings settings;
        ISerialization::SerializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = [&out](const auto &) { return &out; };

        writeIntBinary(static_cast<UInt8>(1), out);
        serialization->serializeBinaryBulkStatePrefix(*column_string, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*column_string, 0, column_string->size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
    }

    auto type_object = std::make_shared<DataTypeObjectDeprecated>("json", false);
    ColumnPtr result_column = type_object->createColumn();

    ReadBufferFromOwnString in(out.str());

    {
        auto serialization = type_object->getDefaultSerialization();

        ISerialization::DeserializeBinaryBulkSettings settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;
        settings.position_independent_encoding = false;
        settings.getter = [&in](const auto &) { return &in; };

        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        serialization->deserializeBinaryBulkWithMultipleStreams(result_column, column_string->size(), settings, state, nullptr);
    }

    auto & column_object = assert_cast<ColumnObjectDeprecated &>(*result_column->assumeMutable());
    column_object.finalize();

    ASSERT_TRUE(column_object.size() == 2);
    ASSERT_TRUE(column_object.getSubcolumns().size() == 4);

    auto check_subcolumn = [&](const auto & name, const auto & type_name, const std::vector<Field> & expected)
    {
        const auto & subcolumn = column_object.getSubcolumn(PathInData{name});
        ASSERT_EQ(subcolumn.getLeastCommonType()->getName(), type_name);

        const auto & data = subcolumn.getFinalizedColumn();
        for (size_t i = 0; i < expected.size(); ++i)
            ASSERT_EQ(
                applyVisitor(FieldVisitorToString(), data[i]),
                applyVisitor(FieldVisitorToString(), expected[i]));
    };

    check_subcolumn("k1", "Int8", {1, 2});
    check_subcolumn("k2.k3", "Array(String)", {Array{"aa", "bb"}, Array{"cc", "", ""}});
    check_subcolumn("k2.k4", "Array(Int8)", {Array{2, 3}, Array{0, 5, 6}});
    check_subcolumn("k2.k5", "Array(Int8)", {Array{0, 0}, Array{4, 0, 0}});
}

#endif
