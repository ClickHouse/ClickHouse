#pragma once

#include <string>
#include <base/types.h>


namespace DB
{

/// Simple numeric version representation.
struct VersionNumber
{
    constexpr VersionNumber() = default;

    constexpr VersionNumber(Int64 major_, Int64 minor_ = 0, Int64 patch_ = 0) // NOLINT(google-explicit-constructor)
        : version_major(major_), version_minor(minor_), version_patch(patch_)
    {
    }

    /// Parse version number from string.
    explicit VersionNumber(std::string version);

    bool operator==(const VersionNumber & rhs) const = default;

    /// There might be negative version code which differs from default comparison.
    auto operator<=>(const VersionNumber & rhs) const { return compare(rhs); }

    std::string toString() const;

private:
    Int64 version_major = 0;
    Int64 version_minor = 0;
    Int64 version_patch = 0;

    int compare(const VersionNumber & rhs) const;
};

}
