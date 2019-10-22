#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <Columns/JSONBDataMark.h>
#include <Columns/ColumnUnique.h>
#include <rapidjson/rapidjson.h>

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
#include <Fleece/Encoder.hh>
#include <Fleece/SharedKeys.hh>
#if !__clang__
#pragma GCC diagnostic pop
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct JSONBinaryStructTracker
{
protected:
    IColumnUnique & keys;
    IColumnUnique & relations;
    std::vector<UInt64> tracker_stack;
    std::vector<UInt64> tracker_relations;

    size_t trackingKey(const char * data, size_t size);

    size_t trackDownKey(const JSONBDataMark & data_mark);

    JSONBinaryStructTracker(IColumnUnique & keys_, IColumnUnique & relations_) : keys(keys_), relations(relations_) {}
};

/// encoder
class JSONBinaryConverter : JSONBinaryStructTracker
{
public:
    typedef char Ch;

    JSONBinaryConverter(IColumnUnique & key_unique_column, IColumnUnique & relations_unique_column);

    void finalizeRelations(IColumn & relations_data_column);

    void finalize(IColumn & relations_data_column, IColumn & binary_data_column);

private:
    fleece::Encoder encoder;

public:
    bool Null();

    bool Bool(bool value);

    bool Int(Int32 value);

    bool Uint(UInt32 value);

    bool Int64(Int64 value);

    bool Uint64(UInt64 value);

    bool Double(Float64 value);

    bool StartObject();

    bool EndObject(rapidjson::SizeType);

    bool Key(const char * data, rapidjson::SizeType length, bool);

    bool String(const char * data, rapidjson::SizeType length, bool);

    bool StartArray() { throw Exception("JSONB type does not support parse array json.", ErrorCodes::NOT_IMPLEMENTED); }

    bool EndArray(rapidjson::SizeType) { throw Exception("JSONB type does not support parse array json.", ErrorCodes::NOT_IMPLEMENTED); }

    bool RawNumber(const Ch *, rapidjson::SizeType, bool) { throw Exception("Method RawNumber is not supported for JSONStructAndColumnBinder.", ErrorCodes::NOT_IMPLEMENTED); }
};

}
