#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <Core/UUID.h>

#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr std::string_view access_target_wire_prefix = "clickhouse:udt-access-target:";
inline constexpr std::string_view access_target_wire_version = "v1";

struct AccessTarget
{
    UUID database_uuid = UUIDHelpers::Nil;
    UUID type_uuid = UUIDHelpers::Nil;

    bool operator==(const AccessTarget &) const = default;
};

class AccessTargetError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        NonCanonical,
    };

    AccessTargetError(Code code_, const String & message_)
        : std::runtime_error(message_)
        , error_code(code_)
    {
    }

    Code code() const noexcept { return error_code; }

private:
    Code error_code;
};

/// Stable AccessRights parameter bytes. The SQL formatter never exposes this wire.
String encodeAccessTarget(const AccessTarget & target);
AccessTarget decodeAccessTarget(std::string_view encoded);

AccessRightsElement makeUsageAccessElement(const AccessTarget & target);

/// TYPE_OBJECT uses a deliberately closed AccessRightsElement shape: either TYPE *
/// (an empty parameter) or one canonical identity. Database/table/column fields,
/// parameter wildcards, and filters are not part of this privilege grammar.
void validateUsageAccessElement(const AccessRightsElement & element);

/// Canonicalizes order and removes duplicate identities before an operation-boundary access check.
AccessRightsElements makeUsageAccessElements(std::span<const AccessTarget> targets);

}
