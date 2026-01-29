#pragma once

#include <optional>
#include <Core/SettingsFields.h>

namespace DB
{

struct SettingFieldOptionalString
{
    std::optional<String> value;

    explicit SettingFieldOptionalString(const std::optional<String> & value_) : value(value_) {}

    explicit SettingFieldOptionalString(const Field & field);

    explicit operator Field() const { return Field(value ? toString(*value) : ""); }
};

}
