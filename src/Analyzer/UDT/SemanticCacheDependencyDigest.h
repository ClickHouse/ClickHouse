#pragma once

#include <DataTypes/UDT/CanonicalHash.h>

#include <Core/Types.h>

#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 semantic_cache_dependency_digest_format_version = 1;

class SemanticRolePlanner;
struct InternedLogicalRole;

struct SemanticCacheDependencyDigestLimits
{
    UInt64 maximum_input_roles = 16'384;
    UInt64 maximum_distinct_roles = 16'384;
    UInt64 maximum_single_arguments_bytes = 64ULL << 10;
    UInt64 maximum_single_shape_bytes = 64ULL << 10;
    UInt64 maximum_input_variable_bytes = 64ULL << 20;
    UInt64 maximum_canonical_encoding_bytes = 64ULL << 20;
    UInt64 maximum_scratch_bytes = 1ULL << 20;
};

class SemanticCacheDependencyError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidRole,
        LimitExceeded,
    };

    SemanticCacheDependencyError(Code code_, std::string_view message);

    const Code code;
};

enum class SemanticCacheDependencyKind : UInt8
{
    /// Existing physical cache namespace: no semantic digest is present or
    /// required. This is distinct from a missing digest for a semantic entry.
    PhysicalOnly,
    /// A nonempty, canonical sorted+unique demanded role set.
    Semantic,
};

/// Canonical expected dependency produced only by a sealed SemanticRolePlanner
/// over its complete authoritative sink enumeration. The digest payload is
/// versioned and streams the exact sorted+unique tuple `(database UUID, type
/// UUID, revision, definition hash, canonical arguments, instantiation
/// semantic hash, normalized logical shape)`. It is move-only so a caller
/// cannot detach a reusable completeness claim from planner result access.
class SemanticCacheDependencyDigest final
{
public:
    SemanticCacheDependencyDigest(const SemanticCacheDependencyDigest &) = delete;
    SemanticCacheDependencyDigest & operator=(const SemanticCacheDependencyDigest &) = delete;
    SemanticCacheDependencyDigest(SemanticCacheDependencyDigest &&) noexcept = default;
    SemanticCacheDependencyDigest & operator=(SemanticCacheDependencyDigest &&) noexcept = default;

    SemanticCacheDependencyKind getKind() const noexcept { return kind; }
    const Digest & getDigest() const noexcept { return digest; }
    UInt64 getRoleCount() const noexcept { return role_count; }
    /// Exact payload bytes after the hash domain separator, excluding any
    /// allocator slack. PhysicalOnly has no semantic payload and returns zero.
    UInt64 getCanonicalEncodingBytes() const noexcept { return canonical_encoding_bytes; }

private:
    SemanticCacheDependencyDigest(
        SemanticCacheDependencyKind kind_, Digest digest_, UInt64 role_count_, UInt64 canonical_encoding_bytes_) noexcept;

    static SemanticCacheDependencyDigest fromSealedPlanner(
        std::span<const InternedLogicalRole * const> roles,
        std::span<const String * const> shapes,
        const SemanticCacheDependencyDigestLimits & limits);
    static SemanticCacheDependencyDigest physicalOnly() noexcept;

    friend class SemanticRolePlanner;

    SemanticCacheDependencyKind kind;
    Digest digest;
    UInt64 role_count;
    UInt64 canonical_encoding_bytes;
};

/// Cache decoder/admission candidate state. Only
/// CompleteCanonical may represent a semantic digest. Partial, stale, and
/// noncanonical states remain explicit even when their bytes happen to equal
/// the expected digest.
enum class SemanticCacheDependencyCandidateState : UInt8
{
    Absent,
    PhysicalOnly,
    CompleteCanonical,
    Partial,
    Stale,
    NonCanonical,
};

struct SemanticCacheDependencyCandidate
{
    SemanticCacheDependencyCandidateState state = SemanticCacheDependencyCandidateState::Absent;
    UInt16 format_version = 0;
    Digest digest{};
    UInt64 role_count = 0;
    UInt64 canonical_encoding_bytes = 0;

    static SemanticCacheDependencyCandidate fromCanonical(const SemanticCacheDependencyDigest & expected) noexcept;
};

enum class SemanticCacheDependencyComparison : UInt8
{
    Match,
    Absent,
    Partial,
    Stale,
    NonCanonical,
    NamespaceMismatch,
    DigestMismatch,
};

/// Pure comparison for cache lookup and admission boundaries. It never
/// treats an absent or malformed semantic dependency as a physical cache hit.
SemanticCacheDependencyComparison
compareSemanticCacheDependency(const SemanticCacheDependencyDigest & expected, const SemanticCacheDependencyCandidate & candidate) noexcept;

}
