#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <base/types.h>

namespace DB
{

/// Simple numeric version representation.
struct VersionNumber
{
    explicit VersionNumber() = default;

    VersionNumber(const std::initializer_list<Int64> & init) : components(init) {}
    explicit VersionNumber(Int64 major, Int64 minor = 0, Int64 patch = 0) : components{major, minor, patch} {}
    explicit VersionNumber(const std::vector<Int64> & components_) : components(components_) {}

    /// Parse version number from string.
    explicit VersionNumber(std::string version);

    bool operator==(const VersionNumber & rhs) const = default;

    /// There might be negative version code which differs from default comparison.
    auto operator<=>(const VersionNumber & rhs) const { return compare(rhs); }

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
