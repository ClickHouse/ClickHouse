#include <DataTypes/JSONB/JSONBinaryConverter.h>
#include <ext/scope_guard.h>
#include <Columns/ColumnArray.h>
#include "JSONBinaryConverter.h"


namespace DB
{

JSONBinaryConverter::JSONBinaryConverter(
    IColumnUnique & key_unique_column, IColumnUnique & relations_unique_column)
    : JSONBinaryStructTracker(key_unique_column, relations_unique_column)
{
}

void JSONBinaryConverter::finalize(IColumn & relations_data_column, IColumn & binary_data_column_)
{
    finalizeRelations(relations_data_column);

    /// TODO: optimize
    auto & binary_data_column = static_cast<ColumnString &>(binary_data_column_);
    ColumnString::Chars & data = binary_data_column.getChars();
    ColumnString::Offsets & offsets = binary_data_column.getOffsets();

    const auto & binary_ref = encoder.extractOutput();
    data.insert(reinterpret_cast<const char *>(binary_ref.buf), reinterpret_cast<const char *>(binary_ref.buf) + binary_ref.size);
    data.push_back(0);
    offsets.push_back(data.size());
}

void JSONBinaryConverter::finalizeRelations(IColumn & relations_data_column_)
{
    auto & relations_data_column = static_cast<ColumnArray &>(relations_data_column_);
    auto & relations_data = static_cast<ColumnUInt64 &>(relations_data_column.getData());
    auto & relations_offset = static_cast<ColumnArray::Offsets &>(relations_data_column.getOffsets());

    relations_data.getData().insert(std::begin(tracker_relations), std::end(tracker_relations));
    relations_offset.push_back(relations_data.size());
}

size_t JSONBinaryStructTracker::trackingKey(const char * data, size_t size)
{
    tracker_stack.push_back(keys.uniqueInsertData(data, size) + UInt64(JSONBDataMark::End));
    return tracker_stack.back();
}

size_t JSONBinaryStructTracker::trackDownKey(const JSONBDataMark & data_mark)
{
    if (data_mark == JSONBDataMark::ObjectMark)
        return 0;

    SCOPE_EXIT({ tracker_stack.pop_back(); });
    tracker_stack.push_back(UInt64(data_mark));
    tracker_relations.push_back(relations.uniqueInsertData(reinterpret_cast<const char *>(tracker_stack.data()), tracker_stack.size() * sizeof(UInt64)));
    return tracker_relations.back();
}

bool JSONBinaryConverter::Null()
{
    encoder.writeNull();
    trackDownKey(JSONBDataMark::NullMark);
    return true;
}

bool JSONBinaryConverter::Bool(bool value)
{
    encoder.writeBool(value);
    trackDownKey(JSONBDataMark::BoolMark);
    return true;
}

bool JSONBinaryConverter::Int(Int32 value)
{
    encoder.writeInt(value);
    trackDownKey(JSONBDataMark::Int64Mark);
    return true;
}

bool JSONBinaryConverter::Uint(UInt32 value)
{
    encoder.writeUInt(value);
    trackDownKey(JSONBDataMark::UInt64Mark);
    return true;
}

bool JSONBinaryConverter::Int64(std::int64_t value)
{
    encoder.writeInt(value);
    trackDownKey(JSONBDataMark::Int64Mark);
    return true;
}

bool JSONBinaryConverter::Uint64(UInt64 value)
{
    encoder.writeUInt(value);
    trackDownKey(JSONBDataMark::UInt64Mark);
    return true;
}

bool JSONBinaryConverter::Double(Float64 value)
{
    encoder.writeDouble(value);
    trackDownKey(JSONBDataMark::Float64Mark);
    return true;
}

bool JSONBinaryConverter::StartObject()
{
    encoder.beginDictionary();
    return true;
}

bool JSONBinaryConverter::EndObject(rapidjson::SizeType)
{
    encoder.endDictionary();
    trackDownKey(JSONBDataMark::ObjectMark);
    return true;
}

bool JSONBinaryConverter::Key(const char * data, rapidjson::SizeType length, bool)
{
    encoder.writeKey(trackingKey(data, length));
    return true;
}

bool JSONBinaryConverter::String(const char * data, rapidjson::SizeType length, bool)
{
    trackDownKey(JSONBDataMark::StringMark);
    encoder.writeString(fleece::slice(data, length));
    return true;
}

}
