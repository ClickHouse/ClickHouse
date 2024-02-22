#pragma once
#include <Core/Types.h>

enum class SQLSecurityType
{
    INVOKER,
    DEFINER,
    NONE,
};
