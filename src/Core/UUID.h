#pragma once

#include <Core/Types.h>


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    const UUID Nil{};
}

}
