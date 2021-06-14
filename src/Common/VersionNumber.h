#pragma once

#include <tuple>
#include <string>
#include <vector>
#include <iostream>

namespace DB
{

/// Simple numeric version representation.
///
/// Based on QVersionNumber.
struct VersionNumber
{
    explicit VersionNumber() = default;

    VersionNumber(const std::initializer_list<long> & init)
        : components(init)
    {}
    VersionNumber(long major, long minor = 0, long patch = 0)
        : components{major, minor, patch}
    {}
    VersionNumber(const std::vector<long> & components_)
        : components(components_)
    {}

    /// Parse version number from string.
    VersionNumber(std::string version);

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
    using Components = std::vector<long>;
    Components components;

    int compare(const VersionNumber & rhs) const;
};

}
