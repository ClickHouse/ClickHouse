#pragma once

#include <Databases/UDT/AuthorityIntegrityVerifier.h>

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/Definition.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>

#include <Core/Types.h>

#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

/// Exact identity of one immutable database authority root. The anchor is
/// part of the identity; an epoch alone never identifies rooted content.
struct AuthorityRootIdentity
{
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 database_catalog_epoch = 0;
    Digest authority_anchor{};

    bool operator==(const AuthorityRootIdentity &) const = default;
};

/// Exact externally observed dependent-object image. This deliberately has
/// no authority, metadata, sidecar, or schema-object handle.
struct AuthorityObjectImageIdentity
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};

    bool operator==(const AuthorityObjectImageIdentity &) const = default;
};

/// A touched-key proof is meaningful only between these two exact root
/// identities. Reuse never accepts an unanchored epoch range.
struct AuthorityRootTransitionIdentity
{
    AuthorityRootIdentity previous;
    AuthorityRootIdentity next;

    bool operator==(const AuthorityRootTransitionIdentity &) const = default;
};

/// Trusted boundary view of one exact atomic authority-root publication. This
/// type does not establish touched-key completeness by itself. It is valid only
/// when populated from the atomic root transition's canonical mutation proof.
/// A caller-assembled or arbitrarily empty key span must not be used to skip
/// integrity verification.
struct AuthorityRootPublicationProofView
{
    AuthorityRootTransitionIdentity transition;
    std::span<const AuthorityInventoryKey> sorted_unique_touched_authority_keys;
};

/// Limits both stamp construction and one pure later-root reuse decision.
/// Retained bytes use the canonical logical widths documented by the model,
/// not allocator capacity or host object sizes.
struct AuthorityVerificationStampLimits
{
    UInt64 maximum_required_definitions = 65'536;
    UInt64 maximum_touched_authority_keys = 131'072;
    UInt64 maximum_work_units = 1'048'576;
    UInt64 maximum_retained_canonical_bytes = 8ULL << 20;
};

struct AuthorityVerificationStampStatistics
{
    UInt64 required_definition_items = 0;
    UInt64 touched_authority_key_items = 0;
    UInt64 work_units = 0;
    UInt64 retained_canonical_bytes = 0;

    bool operator==(const AuthorityVerificationStampStatistics &) const = default;
};

class AuthorityVerificationStampError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        ArithmeticOverflow,
        InvalidVerification,
        InvalidRootIdentity,
        InvalidDefinitionIdentity,
        NonCanonicalDefinitionClosure,
    };

    AuthorityVerificationStampError(Code code_, std::string_view message);

    const Code code;
};

/// Immutable, feature-inert identity proof produced only from a successful
/// object-integrity verification, its exact dependency closure, and the exact
/// root against which that verification ran. It retains no root or record
/// handles and has no publication or invalidation side effect.
class AuthorityVerificationStamp final
{
public:
    using Ptr = std::shared_ptr<const AuthorityVerificationStamp>;

    [[nodiscard]] static Ptr create(
        const VerifiedAuthorityObjectIntegrity & verification,
        std::span<const DefinitionIdentity> sorted_unique_required_definitions,
        const AuthorityRootIdentity & verified_root,
        const AuthorityVerificationStampLimits & limits = {});

    const AuthorityRootIdentity & getVerifiedRoot() const noexcept { return verified_root; }
    const AuthorityObjectImageIdentity & getVerifiedObject() const noexcept { return verified_object; }
    const Digest & getRequiredDefinitionsDigest() const noexcept { return required_definitions_digest; }
    std::span<const DefinitionIdentity> getRequiredDefinitions() const noexcept { return required_definitions; }
    const AuthorityVerificationStampStatistics & getConstructionStatistics() const noexcept { return construction_statistics; }

private:
    AuthorityVerificationStamp(
        AuthorityRootIdentity verified_root_,
        AuthorityObjectImageIdentity verified_object_,
        Digest required_definitions_digest_,
        std::vector<DefinitionIdentity> required_definitions_,
        AuthorityVerificationStampStatistics construction_statistics_) noexcept;

    const AuthorityRootIdentity verified_root;
    const AuthorityObjectImageIdentity verified_object;
    const Digest required_definitions_digest;
    const std::vector<DefinitionIdentity> required_definitions;
    const AuthorityVerificationStampStatistics construction_statistics;
};

enum class AuthorityVerificationStampReuseStatus : UInt8
{
    Reusable,
    RootTransitionUnproven,
    DatabaseChanged,
    ObjectChanged,
    ObjectSchemaRevisionChanged,
    SidecarChanged,
    PhysicalSchemaChanged,
    DependencyClosureUnproven,
    TouchedAuthorityKeysUnproven,
    CoveredAuthorityKeyTouched,
};

struct AuthorityVerificationStampReuseDecision
{
    AuthorityVerificationStampReuseStatus status = AuthorityVerificationStampReuseStatus::RootTransitionUnproven;
    AuthorityVerificationStampStatistics statistics;

    bool isReusable() const noexcept { return status == AuthorityVerificationStampReuseStatus::Reusable; }
};

/// Pure fail-closed decision for carrying one verification stamp to a later
/// root. A reusable result is authoritative only when `publication_proof` was
/// populated by the exact atomic publication boundary described above.
[[nodiscard]] AuthorityVerificationStampReuseDecision decideAuthorityVerificationStampReuse(
    const AuthorityVerificationStamp & stamp,
    const AuthorityRootPublicationProofView & publication_proof,
    const AuthorityObjectImageIdentity & current_object,
    std::span<const DefinitionIdentity> sorted_unique_current_required_definitions,
    const AuthorityVerificationStampLimits & limits = {});

}
