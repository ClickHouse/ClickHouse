#include <Analyzer/UDT/SemanticCacheDependencyDigest.h>
#include <Analyzer/UDT/SemanticRolePlanner.h>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = SemanticCacheDependencyError;

constexpr std::string_view digest_domain = "ClickHouse UDT semantic cache dependency digest V1";
constexpr UInt64 fixed_role_bytes = 2 * sizeof(CanonicalUUID) + sizeof(UInt64) + 2 * sizeof(Digest);

constexpr SemanticCacheDependencyDigestLimits implementation_maxima{
    .maximum_input_roles = 1ULL << 20,
    .maximum_distinct_roles = 1ULL << 20,
    .maximum_single_arguments_bytes = 1ULL << 20,
    .maximum_single_shape_bytes = 1ULL << 20,
    .maximum_input_variable_bytes = 1ULL << 30,
    .maximum_canonical_encoding_bytes = 1ULL << 30,
    .maximum_scratch_bytes = 64ULL << 20,
};

struct PlannerRoleView
{
    const InternedLogicalRole * role = nullptr;
    const String * shape = nullptr;
};

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool binaryLess(std::string_view lhs, std::string_view rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool roleLess(const PlannerRoleView & lhs, const PlannerRoleView & rhs) noexcept
{
    const auto lhs_database = uuidToCanonicalBytes(lhs.role->definition_identity.database_uuid);
    const auto rhs_database = uuidToCanonicalBytes(rhs.role->definition_identity.database_uuid);
    if (lhs_database != rhs_database)
        return lhs_database < rhs_database;

    const auto lhs_type = uuidToCanonicalBytes(lhs.role->definition_identity.type_uuid);
    const auto rhs_type = uuidToCanonicalBytes(rhs.role->definition_identity.type_uuid);
    if (lhs_type != rhs_type)
        return lhs_type < rhs_type;
    if (lhs.role->definition_identity.revision != rhs.role->definition_identity.revision)
        return lhs.role->definition_identity.revision < rhs.role->definition_identity.revision;
    if (lhs.role->definition_hash != rhs.role->definition_hash)
        return lhs.role->definition_hash < rhs.role->definition_hash;
    if (lhs.role->canonical_arguments_encoding != rhs.role->canonical_arguments_encoding)
        return binaryLess(lhs.role->canonical_arguments_encoding, rhs.role->canonical_arguments_encoding);
    if (lhs.role->instantiation_semantic_hash != rhs.role->instantiation_semantic_hash)
        return lhs.role->instantiation_semantic_hash < rhs.role->instantiation_semantic_hash;
    return binaryLess(*lhs.shape, *rhs.shape);
}

bool rolesEqual(const PlannerRoleView & lhs, const PlannerRoleView & rhs) noexcept
{
    return lhs.role->definition_identity == rhs.role->definition_identity && lhs.role->definition_hash == rhs.role->definition_hash
        && lhs.role->canonical_arguments_encoding == rhs.role->canonical_arguments_encoding
        && lhs.role->instantiation_semantic_hash == rhs.role->instantiation_semantic_hash && *lhs.shape == *rhs.shape;
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs * rhs;
}

UInt64 checkedSize(std::size_t size, std::string_view message)
{
    static_assert(sizeof(std::size_t) <= sizeof(UInt64));
    if (!std::in_range<UInt64>(size))
        fail(Error::Code::LimitExceeded, message);
    return static_cast<UInt64>(size);
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        ++result;
        value >>= 7;
    }
    return result;
}

void updateUInt16LE(CanonicalHasher & hasher, UInt16 value)
{
    const std::array<CanonicalByte, sizeof(value)> bytes{
        static_cast<CanonicalByte>(value),
        static_cast<CanonicalByte>(value >> 8),
    };
    hasher.update(bytes);
}

void updateUInt64LE(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, sizeof(value)> bytes{};
    for (size_t index = 0; index < bytes.size(); ++index)
        bytes[index] = static_cast<CanonicalByte>(value >> (8 * index));
    hasher.update(bytes);
}

void updateVarUInt(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, 10> bytes{};
    size_t size = 0;
    do
    {
        CanonicalByte byte = static_cast<CanonicalByte>(value & 0x7f);
        value >>= 7;
        if (value)
            byte = static_cast<CanonicalByte>(byte | 0x80);
        bytes[size++] = byte;
    } while (value);
    hasher.update(std::span(bytes).first(size));
}

void updateFrame(CanonicalHasher & hasher, std::string_view bytes)
{
    updateVarUInt(hasher, checkedSize(bytes.size(), "semantic cache dependency frame does not fit UInt64"));
    hasher.update(bytes);
}

void validateLimits(const SemanticCacheDependencyDigestLimits & limits)
{
    const std::array<UInt64, 7> configured{
        limits.maximum_input_roles,
        limits.maximum_distinct_roles,
        limits.maximum_single_arguments_bytes,
        limits.maximum_single_shape_bytes,
        limits.maximum_input_variable_bytes,
        limits.maximum_canonical_encoding_bytes,
        limits.maximum_scratch_bytes,
    };
    const std::array<UInt64, 7> maxima{
        implementation_maxima.maximum_input_roles,
        implementation_maxima.maximum_distinct_roles,
        implementation_maxima.maximum_single_arguments_bytes,
        implementation_maxima.maximum_single_shape_bytes,
        implementation_maxima.maximum_input_variable_bytes,
        implementation_maxima.maximum_canonical_encoding_bytes,
        implementation_maxima.maximum_scratch_bytes,
    };
    for (size_t index = 0; index < configured.size(); ++index)
    {
        if (configured[index] == 0 || configured[index] > maxima[index])
            fail(Error::Code::InvalidConfiguration, "a semantic cache dependency limit is outside its implementation domain");
    }
    if (limits.maximum_distinct_roles > limits.maximum_input_roles
        || limits.maximum_single_arguments_bytes > limits.maximum_input_variable_bytes
        || limits.maximum_single_shape_bytes > limits.maximum_input_variable_bytes
        || limits.maximum_single_arguments_bytes > limits.maximum_canonical_encoding_bytes
        || limits.maximum_single_shape_bytes > limits.maximum_canonical_encoding_bytes)
    {
        fail(Error::Code::InvalidConfiguration, "a semantic cache dependency item limit exceeds its aggregate limit");
    }
}

void validateRole(const PlannerRoleView & role, const SemanticCacheDependencyDigestLimits & limits, UInt64 & input_variable_bytes)
{
    if (!role.role || !role.shape || role.role->definition_identity.database_uuid == UUIDHelpers::Nil
        || role.role->definition_identity.type_uuid == UUIDHelpers::Nil || role.role->definition_identity.revision == 0
        || isZeroDigest(role.role->definition_hash) || role.role->canonical_arguments_encoding.empty()
        || isZeroDigest(role.role->instantiation_semantic_hash) || role.shape->empty())
    {
        fail(Error::Code::InvalidRole, "a semantic cache dependency role has an incomplete observable identity");
    }

    const UInt64 arguments_size
        = checkedSize(role.role->canonical_arguments_encoding.size(), "semantic cache dependency arguments do not fit UInt64");
    const UInt64 shape_size = checkedSize(role.shape->size(), "semantic cache dependency shape does not fit UInt64");
    if (arguments_size > limits.maximum_single_arguments_bytes || shape_size > limits.maximum_single_shape_bytes)
        fail(Error::Code::LimitExceeded, "a semantic cache dependency variable field exceeds its item byte limit");
    input_variable_bytes = checkedAdd(
        input_variable_bytes,
        checkedAdd(arguments_size, shape_size, "semantic cache dependency input bytes overflow UInt64"),
        "semantic cache dependency input bytes overflow UInt64");
    if (input_variable_bytes > limits.maximum_input_variable_bytes)
        fail(Error::Code::LimitExceeded, "semantic cache dependency input bytes exceed their limit");
}

UInt64 calculateCanonicalBytes(std::span<const PlannerRoleView> roles, UInt64 maximum_bytes)
{
    UInt64 result = checkedAdd(
        sizeof(UInt16),
        varUIntSize(checkedSize(roles.size(), "semantic cache dependency role count does not fit UInt64")),
        "semantic cache dependency encoding bytes overflow UInt64");
    for (const auto & role : roles)
    {
        const UInt64 arguments_size
            = checkedSize(role.role->canonical_arguments_encoding.size(), "semantic cache dependency arguments do not fit UInt64");
        const UInt64 shape_size = checkedSize(role.shape->size(), "semantic cache dependency shape does not fit UInt64");
        UInt64 role_bytes = fixed_role_bytes;
        role_bytes = checkedAdd(role_bytes, varUIntSize(arguments_size), "semantic cache dependency encoding bytes overflow UInt64");
        role_bytes = checkedAdd(role_bytes, arguments_size, "semantic cache dependency encoding bytes overflow UInt64");
        role_bytes = checkedAdd(role_bytes, varUIntSize(shape_size), "semantic cache dependency encoding bytes overflow UInt64");
        role_bytes = checkedAdd(role_bytes, shape_size, "semantic cache dependency encoding bytes overflow UInt64");
        result = checkedAdd(result, role_bytes, "semantic cache dependency encoding bytes overflow UInt64");
        if (result > maximum_bytes)
            fail(Error::Code::LimitExceeded, "semantic cache dependency canonical encoding bytes exceed their limit");
    }
    return result;
}

Digest hashCanonicalRoles(std::span<const PlannerRoleView> roles)
{
    CanonicalHasher hasher(digest_domain);
    updateUInt16LE(hasher, semantic_cache_dependency_digest_format_version);
    updateVarUInt(hasher, static_cast<UInt64>(roles.size()));
    for (const auto & role : roles)
    {
        hasher.updateUUID(role.role->definition_identity.database_uuid);
        hasher.updateUUID(role.role->definition_identity.type_uuid);
        updateUInt64LE(hasher, role.role->definition_identity.revision);
        hasher.update(role.role->definition_hash);
        updateFrame(hasher, role.role->canonical_arguments_encoding);
        hasher.update(role.role->instantiation_semantic_hash);
        updateFrame(hasher, *role.shape);
    }
    return hasher.finalize();
}

}

SemanticCacheDependencyError::SemanticCacheDependencyError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

SemanticCacheDependencyDigest::SemanticCacheDependencyDigest(
    SemanticCacheDependencyKind kind_, Digest digest_, UInt64 role_count_, UInt64 canonical_encoding_bytes_) noexcept
    : kind(kind_)
    , digest(digest_)
    , role_count(role_count_)
    , canonical_encoding_bytes(canonical_encoding_bytes_)
{
}

SemanticCacheDependencyDigest SemanticCacheDependencyDigest::physicalOnly() noexcept
{
    return SemanticCacheDependencyDigest(SemanticCacheDependencyKind::PhysicalOnly, {}, 0, 0);
}

SemanticCacheDependencyDigest SemanticCacheDependencyDigest::fromSealedPlanner(
    std::span<const InternedLogicalRole * const> roles,
    std::span<const String * const> shapes,
    const SemanticCacheDependencyDigestLimits & limits)
{
    validateLimits(limits);
    if (roles.empty())
        return SemanticCacheDependencyDigest::physicalOnly();

    const UInt64 input_role_count = checkedSize(roles.size(), "semantic cache dependency input role count does not fit UInt64");
    if (input_role_count > limits.maximum_input_roles)
        fail(Error::Code::LimitExceeded, "semantic cache dependency input role count exceeds its limit");
    const UInt64 scratch_bytes
        = checkedMultiply(input_role_count, sizeof(PlannerRoleView), "semantic cache dependency scratch bytes overflow UInt64");
    if (scratch_bytes > limits.maximum_scratch_bytes)
        fail(Error::Code::LimitExceeded, "semantic cache dependency scratch bytes exceed their limit");

    std::vector<PlannerRoleView> sorted_roles;
    sorted_roles.reserve(roles.size());
    UInt64 input_variable_bytes = 0;
    for (const auto * role : roles)
    {
        if (!role || role->shape >= shapes.size() || !shapes[role->shape])
            fail(Error::Code::InvalidRole, "a sealed planner role references an unknown owned logical shape");
        PlannerRoleView view{role, shapes[role->shape]};
        validateRole(view, limits, input_variable_bytes);
        sorted_roles.push_back(view);
    }

    std::sort(sorted_roles.begin(), sorted_roles.end(), roleLess);
    const auto unique_end = std::unique(sorted_roles.begin(), sorted_roles.end(), rolesEqual);
    sorted_roles.erase(unique_end, sorted_roles.end());

    const UInt64 role_count = checkedSize(sorted_roles.size(), "semantic cache dependency distinct role count does not fit UInt64");
    if (role_count > limits.maximum_distinct_roles)
        fail(Error::Code::LimitExceeded, "semantic cache dependency distinct role count exceeds its limit");
    const UInt64 canonical_bytes = calculateCanonicalBytes(sorted_roles, limits.maximum_canonical_encoding_bytes);
    auto digest = hashCanonicalRoles(sorted_roles);
    return SemanticCacheDependencyDigest(SemanticCacheDependencyKind::Semantic, std::move(digest), role_count, canonical_bytes);
}

SemanticCacheDependencyCandidate SemanticCacheDependencyCandidate::fromCanonical(const SemanticCacheDependencyDigest & expected) noexcept
{
    return {
        .state = expected.getKind() == SemanticCacheDependencyKind::PhysicalOnly ? SemanticCacheDependencyCandidateState::PhysicalOnly
                                                                                 : SemanticCacheDependencyCandidateState::CompleteCanonical,
        .format_version
        = expected.getKind() == SemanticCacheDependencyKind::PhysicalOnly ? UInt16{0} : semantic_cache_dependency_digest_format_version,
        .digest = expected.getDigest(),
        .role_count = expected.getRoleCount(),
        .canonical_encoding_bytes = expected.getCanonicalEncodingBytes(),
    };
}

SemanticCacheDependencyComparison
compareSemanticCacheDependency(const SemanticCacheDependencyDigest & expected, const SemanticCacheDependencyCandidate & candidate) noexcept
{
    switch (candidate.state)
    {
        case SemanticCacheDependencyCandidateState::Absent: return SemanticCacheDependencyComparison::Absent;
        case SemanticCacheDependencyCandidateState::Partial: return SemanticCacheDependencyComparison::Partial;
        case SemanticCacheDependencyCandidateState::Stale: return SemanticCacheDependencyComparison::Stale;
        case SemanticCacheDependencyCandidateState::NonCanonical: return SemanticCacheDependencyComparison::NonCanonical;
        case SemanticCacheDependencyCandidateState::PhysicalOnly: {
            if (candidate.format_version != 0 || !isZeroDigest(candidate.digest) || candidate.role_count != 0
                || candidate.canonical_encoding_bytes != 0)
                return SemanticCacheDependencyComparison::NonCanonical;
            return expected.getKind() == SemanticCacheDependencyKind::PhysicalOnly ? SemanticCacheDependencyComparison::Match
                                                                                   : SemanticCacheDependencyComparison::NamespaceMismatch;
        }
        case SemanticCacheDependencyCandidateState::CompleteCanonical: {
            if (candidate.format_version != semantic_cache_dependency_digest_format_version || candidate.role_count == 0
                || candidate.canonical_encoding_bytes == 0)
                return SemanticCacheDependencyComparison::NonCanonical;
            if (expected.getKind() != SemanticCacheDependencyKind::Semantic)
                return SemanticCacheDependencyComparison::NamespaceMismatch;
            if (candidate.role_count != expected.getRoleCount()
                || candidate.canonical_encoding_bytes != expected.getCanonicalEncodingBytes() || candidate.digest != expected.getDigest())
            {
                return SemanticCacheDependencyComparison::DigestMismatch;
            }
            return SemanticCacheDependencyComparison::Match;
        }
    }
    return SemanticCacheDependencyComparison::NonCanonical;
}

}
