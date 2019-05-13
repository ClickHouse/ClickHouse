#pragma once

#include <Core/Field.h>


namespace DB
{

struct SettingChange
{
    String name;
    Field value;
};

using SettingsChanges = std::vector<SettingChange>;

}
