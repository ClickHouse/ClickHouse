#pragma once

#include <tuple>
#include <string>
#include <vector>
#include <iostream>

namespace DB
{

/// Simple numeric version representation.
///
/// Supports only "major.minor.patch", other components are ignored.
struct VersionNumber
{
    explicit VersionNumber() = default;

    VersionNumber(const std::tuple<long, long, long> & ver)
        : version(ver)
    {}
    VersionNumber(const std::initializer_list<long> & init)
        : VersionNumber(std::vector<long>(init))
    {}
    VersionNumber(long major, long minor, long patch)
        : version(major, minor, patch)
    {}

    VersionNumber(const std::vector<long> & vec);

    /// NOTE: operator<=> can be used once libc++ will be upgraded.
    bool operator<(const VersionNumber & rhs)  const { return version <  rhs.version; }
    bool operator<=(const VersionNumber & rhs) const { return version <= rhs.version; }
    bool operator==(const VersionNumber & rhs) const { return version == rhs.version; }
    bool operator>(const VersionNumber & rhs)  const { return version >  rhs.version; }
    bool operator>=(const VersionNumber & rhs) const { return version >= rhs.version; }

    std::string toString() const;

    friend std::ostream & operator<<(std::ostream & os, const VersionNumber & v)
    {
        return os << v.toString();
    }

    /// @param strict - throws if number of components > 3
    static VersionNumber fromString(std::string version, bool strict);

private:
    using VersionTuple = std::tuple<long, long, long>;
    static constexpr size_t SIZE = std::tuple_size<VersionTuple>();

    VersionTuple version{};
};

}
