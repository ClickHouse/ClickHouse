#pragma once

#include <common/strong_typedef.h>
#include <common/extended_types.h>


namespace DB
{

STRONG_TYPEDEF(UInt128, UUID)

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    const UUID Nil{};
}

}
