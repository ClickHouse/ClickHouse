#pragma once

#include <DataTypes/DataTypeNumberBase.h>


namespace DB
{

class DataTypeDate final : public DataTypeNumberBase<UInt16>
{
public:
    static constexpr auto family_name = "Date";

    TypeIndex getTypeId() const override { return TypeIndex::Date; }
    const char * getFamilyName() const override { return family_name; }

    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType & rhs) const override;

protected:
    SerializationPtr doGetDefaultSerialization() const override;
};

class DataTypeDate32;
class DataTypeDateTime;
class DataTypeDateTime64;

namespace dt
{
template <class T>
concept DateOrDateTime =
    std::is_same_v<T, DataTypeDate>
    || std::is_same_v<T, DataTypeDate32>
    || std::is_same_v<T, DataTypeDateTime>
    || std::is_same_v<T, DataTypeDateTime64>;
}
}
