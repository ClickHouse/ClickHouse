#pragma once

#include <base/strong_typedef.h>
#include <base/extended_types.h>

namespace DB
{
    using IPv4 = StrongTypedef<UInt32, struct IPv4Tag>;
    using IPv6 = StrongTypedef<UInt128, struct IPv6Tag>;
}
