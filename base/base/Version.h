#pragma once

#include <base/extended_types.h>
#include <base/strong_typedef.h>


namespace DB
{

    /// A 4-component numeric version: `major.minor.patch.build`, each a `UInt32`, packed into a
    /// `UInt128` as `(major << 96) | (minor << 64) | (patch << 32) | build` and compared as a plain
    /// integer. Missing trailing components default to 0, so `1.2` is equal to `1.2.0.0`.
    ///
    /// This is deliberately NOT semver: there is no support for pre-release (`-alpha`) or
    /// build-metadata (`+build`) suffixes, since neither has a value that fits into the fixed
    /// numeric packing above without either running out of spare bits or giving up free integer
    /// ordering. Text containing such a suffix is rejected with `CANNOT_PARSE_VERSION` rather than
    /// silently truncated. This model is closer to `ClickHouse`'s own version scheme
    /// (`24.3.1.2672`) than to full semver.
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
