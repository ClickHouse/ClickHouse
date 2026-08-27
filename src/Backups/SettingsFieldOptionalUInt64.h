#pragma once

#include <optional>
#include <Core/SettingsFields.h>

namespace DB
{

struct SettingFieldOptionalUInt64
{
    std::optional<UInt64> value;

    explicit SettingFieldOptionalUInt64(const std::optional<UInt64> & value_) : value(value_) {}

    explicit SettingFieldOptionalUInt64(const Field & field);

    explicit operator Field() const { return Field(value ? toString(*value) : ""); }
};

}
