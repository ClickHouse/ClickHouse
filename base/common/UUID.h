#pragma once

#include "StrongTypedef.h"
#include "extended_types.h"

namespace DB
{
using UUID = StrongTypedef<UInt128, struct UUIDTag>;
}
