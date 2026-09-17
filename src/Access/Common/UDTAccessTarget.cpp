#include <Access/Common/UDTAccessTarget.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

constexpr size_t canonical_uuid_size = 36;
constexpr size_t encoded_size = access_target_wire_prefix.size()
    + access_target_wire_version.size() + 1 + canonical_uuid_size + 1 + canonical_uuid_size;

[[noreturn]] void throwTargetError(AccessTargetError::Code code, std::string_view message)
{
    throw AccessTargetError(code, String{message});
}

UUID parseCanonicalUUID(std::string_view text)
{
    UUID value;
    try
    {
        value = parseFromString<UUID>(text);
    }
    catch (const std::exception &)
    {
        throwTargetError(AccessTargetError::Code::InvalidValue, "Invalid UUID in a user-defined type access target");
    }

    if (value == UUIDHelpers::Nil)
        throwTargetError(AccessTargetError::Code::InvalidValue, "Nil UUID in a user-defined type access target");

    if (toString(value) != text)
        throwTargetError(AccessTargetError::Code::NonCanonical, "Non-canonical UUID in a user-defined type access target");

    return value;
}

}

String encodeAccessTarget(const AccessTarget & target)
{
    if (target.database_uuid == UUIDHelpers::Nil || target.type_uuid == UUIDHelpers::Nil)
        throwTargetError(
            AccessTargetError::Code::InvalidValue, "A user-defined type access target cannot contain a nil UUID");

    String encoded;
    encoded.reserve(encoded_size);
    encoded += access_target_wire_prefix;
    encoded += access_target_wire_version;
    encoded += ':';
    encoded += toString(target.database_uuid);
    encoded += ':';
    encoded += toString(target.type_uuid);
    return encoded;
}

AccessTarget decodeAccessTarget(std::string_view encoded)
{
    if (encoded.size() < access_target_wire_prefix.size())
        throwTargetError(AccessTargetError::Code::Truncated, "Truncated user-defined type access target");

    if (!encoded.starts_with(access_target_wire_prefix))
        throwTargetError(AccessTargetError::Code::InvalidValue, "Invalid user-defined type access target prefix");

    const size_t version_begin = access_target_wire_prefix.size();
    const size_t version_end = encoded.find(':', version_begin);
    if (version_end == std::string_view::npos)
        throwTargetError(AccessTargetError::Code::Truncated, "Truncated user-defined type access target version");

    if (encoded.substr(version_begin, version_end - version_begin) != access_target_wire_version)
        throwTargetError(AccessTargetError::Code::UnsupportedVersion, "Unsupported user-defined type access target version");

    if (encoded.size() < encoded_size)
        throwTargetError(AccessTargetError::Code::Truncated, "Truncated user-defined type access target");
    if (encoded.size() > encoded_size)
        throwTargetError(AccessTargetError::Code::NonCanonical, "Trailing bytes in a user-defined type access target");

    const size_t database_uuid_begin = version_end + 1;
    const size_t type_uuid_separator = database_uuid_begin + canonical_uuid_size;
    if (encoded[type_uuid_separator] != ':')
        throwTargetError(AccessTargetError::Code::InvalidValue, "Invalid user-defined type access target separator");

    const auto database_uuid_text = encoded.substr(database_uuid_begin, canonical_uuid_size);
    const auto type_uuid_text = encoded.substr(type_uuid_separator + 1, canonical_uuid_size);
    return {
        .database_uuid = parseCanonicalUUID(database_uuid_text),
        .type_uuid = parseCanonicalUUID(type_uuid_text),
    };
}

AccessRightsElement makeUsageAccessElement(const AccessTarget & target)
{
    AccessRightsElement element{AccessType::USAGE_TYPE};
    element.parameter = encodeAccessTarget(target);
    return element;
}

void validateUsageAccessElement(const AccessRightsElement & element)
{
    if (element.access_flags.getParameterType() != AccessFlags::TYPE_OBJECT)
        throwTargetError(AccessTargetError::Code::InvalidValue, "Expected TYPE_OBJECT access flags");

    if (!element.database.empty() || !element.table.empty() || !element.columns.empty() || !element.filter.empty()
        || element.default_database || element.wildcard)
        throwTargetError(
            AccessTargetError::Code::InvalidValue,
            "A user-defined type access target cannot contain database, table, column, wildcard, or filter fields");

    if (!element.parameter.empty())
        static_cast<void>(decodeAccessTarget(element.parameter));
}

AccessRightsElements makeUsageAccessElements(std::span<const AccessTarget> targets)
{
    std::vector<String> encoded_targets;
    encoded_targets.reserve(targets.size());
    for (const auto & target : targets)
        encoded_targets.emplace_back(encodeAccessTarget(target));

    std::sort(encoded_targets.begin(), encoded_targets.end());
    encoded_targets.erase(std::unique(encoded_targets.begin(), encoded_targets.end()), encoded_targets.end());

    AccessRightsElements elements;
    elements.reserve(encoded_targets.size());
    for (auto & encoded_target : encoded_targets)
    {
        AccessRightsElement element{AccessType::USAGE_TYPE};
        element.parameter = std::move(encoded_target);
        elements.emplace_back(std::move(element));
    }
    return elements;
}

}
