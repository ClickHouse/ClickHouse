#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationReplicated.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/ThreadStatus.h>

#include <gtest/gtest.h>

using namespace DB;

/// ColumnReplicated keeps a nested column plus an index; rows of the nested column that no index entry
/// references are dropped just before serialization. With flattened Dynamic Native serialization the nested
/// serializer caches a column-derived state in serializeBinaryBulkStatePrefix. The state must be built from
/// the already-compacted nested column.
TEST(ReplicatedSerialization, FlattenedDynamicInsideReplicatedNativeRoundTrip)
{
    MainThreadStatus::getInstance();

    auto type = DataTypeFactory::instance().get("Dynamic");

    auto nested = type->createColumn();
    nested->insert(Field(static_cast<UInt64>(100)));
    nested->insert(Field(static_cast<UInt64>(200)));
    nested->insert(Field("hello"));

    auto indexes = ColumnUInt8::create();
    indexes->insert(Field(static_cast<UInt64>(0)));
    indexes->insert(Field(static_cast<UInt64>(2)));

    auto column = ColumnReplicated::create(std::move(nested), std::move(indexes));

    auto serialization = SerializationReplicated::create(type->getDefaultSerialization());

    FormatSettings format_settings;
    format_settings.native.use_flattened_dynamic_and_json_serialization = true;

    WriteBufferFromOwnString out;
    {
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&out](ISerialization::SubstreamPath) -> WriteBuffer * { return &out; };
        settings.native_format = true;
        settings.format_settings = &format_settings;

        ISerialization::SerializeBinaryBulkStatePtr state;
        serialization->serializeBinaryBulkStatePrefix(*column, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, column->size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
    }

    ColumnPtr result = ColumnReplicated::create(type->createColumn());
    {
        ReadBufferFromString in(out.str());
        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&in](ISerialization::SubstreamPath) -> ReadBuffer * { return &in; };
        settings.native_format = true;
        settings.format_settings = &format_settings;

        ISerialization::DeserializeBinaryBulkStatePtr state;
        serialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        serialization->deserializeBinaryBulkWithMultipleStreams(result, 0, column->size(), settings, state, nullptr);
    }

    ASSERT_EQ(result->size(), column->size());
    for (size_t i = 0; i < column->size(); ++i)
        ASSERT_EQ((*result)[i], (*column)[i]);
}
