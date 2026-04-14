#pragma once

#include <optional>
#include <Core/SettingsFields.h>

namespace DB
{

struct SettingFieldOptionalBool
{
    std::optional<bool> value;

    explicit SettingFieldOptionalBool(const std::optional<bool> & value_) : value(value_) {}

    explicit SettingFieldOptionalBool(const Field & field);

    explicit operator Field() const;
};

}
