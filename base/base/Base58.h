#pragma once

#include <base/strong_typedef.h>
#include <base/extended_types.h>

namespace DB
{
    using Base58 = StrongTypedef<DB::String, struct Base58Type>;
}
