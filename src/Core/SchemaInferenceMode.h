#pragma once

#include <cstdint>

namespace DB
{

enum class SchemaInferenceMode : uint8_t
{
    DEFAULT,
    UNION,
};

}
