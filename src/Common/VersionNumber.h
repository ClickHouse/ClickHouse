#pragma once

#include <string>
#include <vector>
#include <base/types.h>


namespace DB
{

/// Simple numeric version representation.
struct VersionNumber
{
    explicit VersionNumber() = default;

    constexpr VersionNumber(const std::initializer_list<Int64> & init) : components(init) {}
    explicit VersionNumber(Int64 major, Int64 minor = 0, Int64 patch = 0) : components{major, minor, patch} { }

    /// Parse version number from string.
    explicit VersionNumber(std::string version);

    bool operator==(const VersionNumber & rhs) const = default;

    /// There might be negative version code which differs from default comparison.
    auto operator<=>(const VersionNumber & rhs) const { return compare(rhs); }

    std::string toString() const;

private:
    using Components = std::vector<Int64>;
    Components components;

    int compare(const VersionNumber & rhs) const;
};

}
