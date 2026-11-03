#pragma once

#include <base/EnumReflection.h>

#include <optional>
#include <string>
#include <string_view>

/// Upload checksum algorithm for S3 requests, plus the pure (AWS-SDK-free) helpers that
/// operate on it. Kept separate from `Requests.h` so it can be used from translation units
/// that are compiled without `USE_AWS_S3` (e.g. `S3RequestSettings.cpp`).
namespace DB::S3::RequestChecksum
{

/// The enum member names ARE the accepted `upload_checksum_algorithm` setting spellings,
/// so this enum is the single source of truth: parsing and validation derive from it via
/// `magic_enum`, with no separate hand-maintained list of names that could drift out of sync.
enum class Algorithm
{
    /// User-visible `MD5` mode. AWS represents it through the SDK `Content-MD5` path,
    /// not through `ChecksumAlgorithm` / `x-amz-checksum-*`.
    /// With checksums disabled it means no checksum at all.
    MD5,
    CRC32,
    SHA256,
};

inline bool usesFlexibleChecksumHeader(Algorithm algorithm)
{
    return algorithm == Algorithm::CRC32 || algorithm == Algorithm::SHA256;
}

/// Parse a (normalized, upper-cased) setting value into the enum. Returns `std::nullopt` for
/// an unknown value. An empty value is the caller's "use the environment default" sentinel and
/// is not handled here.
inline std::optional<Algorithm> tryParse(std::string_view name)
{
    return magic_enum::enum_cast<Algorithm>(name);
}

/// Comma-separated list of accepted spellings, derived from the enum, for error messages.
inline std::string supportedAlgorithms()
{
    std::string result;
    for (const auto & name : magic_enum::enum_names<Algorithm>())
    {
        if (!result.empty())
            result += ", ";
        result += name;
    }
    return result;
}

}
