#include <Core/SettingFieldDataType.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

SettingFieldDataType::SettingFieldDataType(const DataTypePtr & type)
    : value{type}
{
}

SettingFieldDataType::SettingFieldDataType(const String & str)
    : value{str.empty() ? nullptr : DataTypeFactory::instance().get(str)}
{
}

SettingFieldDataType::SettingFieldDataType(const Field & f)
    : SettingFieldDataType{f.safeGet<String>()}
{
}

SettingFieldDataType & SettingFieldDataType::operator =(const String & str)
{
    value = str.empty() ? nullptr : DataTypeFactory::instance().get(str);
    changed = true;
    return *this;
}

SettingFieldDataType & SettingFieldDataType::operator =(const Field & f)
{
    *this = f.safeGet<String>();
    return *this;
}

String SettingFieldDataType::toString() const
{
    return value ? value->getName() : String{};
}

void SettingFieldDataType::parseFromString(const String & str)
{
    *this = str;
}

void SettingFieldDataType::writeBinary(WriteBuffer & out) const
{
    bool has_value = (value != nullptr);
    ::DB::writeBinary(has_value, out);
    if (has_value)
        encodeDataType(value, out);
}

void SettingFieldDataType::readBinary(ReadBuffer & in)
{
    bool has_value;
    ::DB::readBinary(has_value, in);
    value = has_value ? decodeDataType(in) : nullptr;
    changed = true;
}

}
