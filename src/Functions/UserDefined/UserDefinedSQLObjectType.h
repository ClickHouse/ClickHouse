#pragma once

#include <cstdint>

namespace DB
{

enum class UserDefinedSQLObjectType : uint8_t
{
    Function,
    Type
};

}
