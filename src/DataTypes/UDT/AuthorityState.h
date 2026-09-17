#pragma once

#include <DataTypes/UDT/CanonicalHash.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 authority_state_format_version = 1;

enum class AuthorityCapability : UInt64
{
    DefinitionRecords = UInt64{1} << 0,
    ReferenceSidecars = UInt64{1} << 1,
    SidecarExpectations = UInt64{1} << 2,
    DependentObjectWALDeltas = UInt64{1} << 3,
    SchemaGraph = UInt64{1} << 4,
};

inline constexpr UInt64 definition_authority_capability_mask = static_cast<UInt64>(AuthorityCapability::DefinitionRecords);
inline constexpr UInt64 dependent_object_authority_capability_mask = definition_authority_capability_mask
    | static_cast<UInt64>(AuthorityCapability::ReferenceSidecars) | static_cast<UInt64>(AuthorityCapability::SidecarExpectations)
    | static_cast<UInt64>(AuthorityCapability::DependentObjectWALDeltas) | static_cast<UInt64>(AuthorityCapability::SchemaGraph);

struct AuthorityState
{
    UInt16 format_version = authority_state_format_version;
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 database_catalog_epoch = 0;
    UInt64 persistent_capability_mask = 0;
    UInt64 leaf_count = 0;
    Digest inventory_root{};
    Digest schema_graph_root{};
    Digest anchor_hash{};

    bool operator==(const AuthorityState &) const = default;
};

struct AuthorityStateLimits
{
    UInt64 maximum_leaves = 200'000;
    UInt64 maximum_encoded_bytes = 256;
};

class AuthorityStateError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
    };

    AuthorityStateError(Code code_, std::string_view message);

    const Code code;
};

/// Computes the anchor over every preceding field in the canonical V1 state.
Digest computeAuthorityStateAnchor(const AuthorityState & state, const AuthorityStateLimits & limits = {});

/// Constructs a state and fills its anchor. Only the complete definition-only
/// and dependent-object capability sets are admitted, never an intermediate set.
AuthorityState makeAuthorityState(
    UUID database_uuid,
    UInt64 database_catalog_epoch,
    UInt64 persistent_capability_mask,
    UInt64 leaf_count,
    Digest inventory_root,
    Digest schema_graph_root,
    const AuthorityStateLimits & limits = {});

/// The only dependent-object activation transition. It advances the database
/// epoch once and changes only the complete capability set and anchor.
AuthorityState activateDependentObjectAuthority(const AuthorityState & definition_state, const AuthorityStateLimits & limits = {});

String encodeAuthorityState(const AuthorityState & state, const AuthorityStateLimits & limits = {});
AuthorityState decodeAuthorityState(std::string_view bytes, const AuthorityStateLimits & limits = {});

}
