#include <DataTypes/JSONB/JSONBDataBinder.h>

namespace DB
{

bool JSONBDataBinder::Null()
{
    encoder.writeNull();
    return true;
}

bool JSONBDataBinder::Bool(bool value)
{
    encoder.writeBool(value);
    return true;
}

bool JSONBDataBinder::Int(Int32 value)
{
    encoder.writeInt(value);
    return true;
}

bool JSONBDataBinder::Uint(UInt32 value)
{
    encoder.writeUInt(value);
    return true;
}

bool JSONBDataBinder::Int64(Int64 value)
{
    encoder.writeInt(value);
    return true;
}

bool JSONBDataBinder::Uint64(UInt64 value)
{
    encoder.writeUInt(value);
    return true;
}

bool JSONBDataBinder::Double(Float64 value)
{
    encoder.writeDouble(value);
    return true;
}

bool JSONBDataBinder::StartObject()
{
    encoder.beginDictionary();
    return true;
}

bool JSONBDataBinder::EndObject(rapidjson::SizeType)
{
    encoder.endDictionary();
    return true;
}

bool JSONBDataBinder::Key(const char * data, rapidjson::SizeType length, bool)
{
    encoder.writeKey(fleece::slice(data, length));
    return true;
}

bool JSONBDataBinder::String(const char * data, rapidjson::SizeType length, bool)
{
    encoder.writeString(fleece::slice(data, length));
    return true;
}


}
