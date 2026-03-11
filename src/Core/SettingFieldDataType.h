#pragma once

#include <Core/SettingsFields.h>
#include <DataTypes/IDataType.h>


namespace DB
{

/// Represents DataTypePtr, can be parsed from string.
struct SettingFieldDataType final : SettingFieldBase
{
    DataTypePtr value;
    bool changed = false;

    explicit SettingFieldDataType(const DataTypePtr & type = {});
    explicit SettingFieldDataType(const String & str);
    explicit SettingFieldDataType(const Field & f);

    SettingFieldDataType(const SettingFieldDataType & o)
        : value(o.value), changed(o.changed)
    {}

    SettingFieldDataType & operator =(const DataTypePtr & type) { value = type; changed = true; return *this; }
    SettingFieldDataType & operator =(const String & str);
    SettingFieldDataType & operator =(const Field & f) override;

    SettingFieldDataType & operator =(const SettingFieldDataType & o)
    {
        if (this != &o)
        {
            value = o.value;
            changed = o.changed;
        }
        return *this;
    }

    bool isChanged() const override { return changed; }
    void setChanged(bool changed_) override { changed = changed_; }

    operator const DataTypePtr &() const { return value; } /// NOLINT
    explicit operator bool() const { return value != nullptr; }
    explicit operator Field() const override { return toString(); }

    String toString() const override;
    void parseFromString(const String & str) override;

    void writeBinary(WriteBuffer & out) const override;
    void readBinary(ReadBuffer & in) override;
};

}
