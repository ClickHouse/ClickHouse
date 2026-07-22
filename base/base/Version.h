#pragma once

#include <base/extended_types.h>
#include <base/strong_typedef.h>


namespace DB
{

    struct Version : StrongTypedef<UInt128, struct VersionTag>
    {
        using StrongTypedef::StrongTypedef;
        using StrongTypedef::operator=;
    };

}

namespace std
{
    template <>
    struct hash<DB::Version>
    {
        size_t operator()(const DB::Version & x) const
        {
            return std::hash<DB::Version::UnderlyingType>()(x.toUnderType());
        }
    };
}
