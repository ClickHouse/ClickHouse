#pragma once

#include <base/types.h>

namespace DB
{

struct ReadFromProjectionSettings
{
    String name;

    bool operator==(const ReadFromProjectionSettings & rhs) const = default;
};

}
