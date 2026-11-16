#include <Core/SettingFieldDataType.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace
{

DataTypePtr parseDataType(const String & str)
{
    if (str.empty())
        return nullptr;
    return DataTypeFactory::instance().get(str);
}

}

SettingFieldDataType::SettingFieldDataType(const DataTypePtr & type)
    : value{type}
{
}

SettingFieldDataType::SettingFieldDataType(const String & str)
    : value{parseDataType(str)}
{
}

SettingFieldDataType::SettingFieldDataType(const Field & f)
    : SettingFieldDataType{f.safeGet<String>()}
{
}

SettingFieldDataType & SettingFieldDataType::operator =(const String & str)
{
    value = parseDataType(str);
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
    writeStringBinary(toString(), out);
}

void SettingFieldDataType::readBinary(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    *this = str;
}

}
