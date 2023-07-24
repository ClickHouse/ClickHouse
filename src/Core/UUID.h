#pragma once

#include <Core/Types.h>


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    inline void toCompatibleFormat(UUID & uuid)
    {
        auto & impl = uuid.toUnderType();
        std::swap(impl.items[0], impl.items[1]);
    }

    const UUID Nil{};
}

}
