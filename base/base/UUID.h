#pragma once

#include "strong_typedef.h"
#include "extended_types.h"

namespace DB
{
    using UUID = StrongTypedef<UInt128, struct UUIDTag>;
}
