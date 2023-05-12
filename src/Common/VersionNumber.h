#pragma once

#include <tuple>
#include <string>
#include <vector>
#include <iostream>
#include <base/types.h>

namespace DB
{

/// Simple numeric version representation.
///
/// Based on QVersionNumber.
struct VersionNumber
{
    explicit VersionNumber() = default;

    VersionNumber(const std::initializer_list<Int64> & init)
        : components(init)
    {}
    VersionNumber(Int64 major, Int64 minor = 0, Int64 patch = 0) /// NOLINT
        : components{major, minor, patch}
    {}
    VersionNumber(const std::vector<Int64> & components_) /// NOLINT
        : components(components_)
    {}

    /// Parse version number from string.
    explicit VersionNumber(std::string version);

    /// NOTE: operator<=> can be used once libc++ will be upgraded.
    bool operator<(const VersionNumber & rhs)  const { return compare(rhs.components) <  0; }
    bool operator<=(const VersionNumber & rhs) const { return compare(rhs.components) <= 0; }
    bool operator==(const VersionNumber & rhs) const { return compare(rhs.components) == 0; }
    bool operator>(const VersionNumber & rhs)  const { return compare(rhs.components) >  0; }
    bool operator>=(const VersionNumber & rhs) const { return compare(rhs.components) >= 0; }

    std::string toString() const;
    friend std::ostream & operator<<(std::ostream & os, const VersionNumber & v)
    {
        return os << v.toString();
    }

private:
    using Components = std::vector<Int64>;
    Components components;

    int compare(const VersionNumber & rhs) const;
};

}
