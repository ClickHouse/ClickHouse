#pragma once

#include <Core/SettingsFields.h>
#include <DataTypes/IDataType_fwd.h>


namespace DB
{

/// Represents a data type, can be parsed from string (the name of the type),
/// outputs to string as the name of the type (so it can be parsed back).
struct SettingFieldDataType
{
    DataTypePtr value;
    bool changed = false;

    explicit SettingFieldDataType(const DataTypePtr & type = {});
    explicit SettingFieldDataType(const String & str);
    explicit SettingFieldDataType(const Field & f);

    SettingFieldDataType(const SettingFieldDataType &) = default;
    SettingFieldDataType & operator =(const SettingFieldDataType &) = default;

    SettingFieldDataType & operator =(const DataTypePtr & type) { value = type; changed = true; return *this; }
    SettingFieldDataType & operator =(const String & str);
    SettingFieldDataType & operator =(const Field & f);

    bool isChanged() const { return changed; }
    void setChanged(bool changed_) { changed = changed_; }

    operator const DataTypePtr &() const { return value; } /// NOLINT
    explicit operator bool() const { return value != nullptr; }
    explicit operator Field() const { return toString(); }

    String toString() const;
    void parseFromString(const String & str);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);
};

}
