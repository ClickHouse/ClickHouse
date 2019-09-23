#pragma once

#include <Core/Types.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <rapidjson/rapidjson.h>

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif
#include <Fleece/Encoder.hh>
#if !__clang__
#pragma GCC diagnostic pop
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class JSONBDataBinder
{
public:
    typedef char Ch;

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

    fleece::slice finalize();

private:
    fleece::Encoder encoder;
};

}
