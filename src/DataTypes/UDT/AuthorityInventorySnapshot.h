#pragma once

#include <DataTypes/UDT/AuthorityInventory.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

inline constexpr UInt16 authority_inventory_snapshot_format_version = 1;

struct AuthorityInventorySnapshot
{
    UInt16 format_version = authority_inventory_snapshot_format_version;
    UUID database_uuid = UUIDHelpers::Nil;
    std::vector<AuthorityInventoryLeaf> leaves;
    UInt16 semantic_extension_version = 1;
    UInt16 semantic_extension_flags = 0;

    bool operator==(const AuthorityInventorySnapshot &) const = default;
};

struct AuthorityInventorySnapshotLimits
{
    AuthorityInventoryLimits inventory;
    UInt64 maximum_snapshot_bytes = 32ULL << 20;
};

class AuthorityInventorySnapshotError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
    };

    AuthorityInventorySnapshotError(Code code_, std::string_view message);

    const Code code;
};

AuthorityInventorySnapshot makeAuthorityInventorySnapshot(
    UUID database_uuid, std::vector<AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventorySnapshotLimits & limits = {});

String
encodeAuthorityInventorySnapshot(const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits = {});

AuthorityInventorySnapshot
decodeAuthorityInventorySnapshot(std::string_view bytes, const AuthorityInventorySnapshotLimits & limits = {});

Digest computeAuthorityInventorySnapshotHash(
    const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits = {});

AuthorityInventory::Ptr
buildAuthorityInventoryFromSnapshot(const AuthorityInventorySnapshot & snapshot, const AuthorityInventorySnapshotLimits & limits = {});

}
