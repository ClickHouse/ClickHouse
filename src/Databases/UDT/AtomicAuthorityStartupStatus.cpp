#include <Databases/UDT/AtomicAuthorityStartupStatus.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace DB::UDT
{
namespace
{

bool containsZero(std::string_view value) noexcept
{
    return value.find('\0') != std::string_view::npos;
}

bool isDegradedStatus(AuthorityDefinitionStatus status) noexcept
{
    return status == AuthorityDefinitionStatus::Conflicted || status == AuthorityDefinitionStatus::Invalid
        || status == AuthorityDefinitionStatus::Incomplete;
}

}

std::string_view getAuthorityDefinitionStatusName(AuthorityDefinitionStatus status) noexcept
{
    switch (status)
    {
        case AuthorityDefinitionStatus::Active: return "ACTIVE";
        case AuthorityDefinitionStatus::Conflicted: return "CONFLICTED";
        case AuthorityDefinitionStatus::Invalid: return "INVALID";
        case AuthorityDefinitionStatus::Incomplete: return "INCOMPLETE";
        case AuthorityDefinitionStatus::Quarantined: return "QUARANTINED";
        case AuthorityDefinitionStatus::OverQuota: return "OVER_QUOTA";
    }
    return "INCOMPLETE";
}

AtomicAuthorityStartupStatusSnapshot::AtomicAuthorityStartupStatusSnapshot(
    UUID database_uuid_,
    std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics_,
    std::vector<AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects_,
    std::vector<size_t> expected_dependent_objects_by_name_,
    AtomicAuthorityStartupDependentObjectScope dependent_object_scope_,
    AuthorityDefinitionStatus global_status_,
    String global_last_error_) noexcept
    : database_uuid(database_uuid_)
    , diagnostics(std::move(diagnostics_))
    , expected_dependent_objects(std::move(expected_dependent_objects_))
    , expected_dependent_objects_by_name(std::move(expected_dependent_objects_by_name_))
    , dependent_object_scope(dependent_object_scope_)
    , global_status(global_status_)
    , global_last_error(std::move(global_last_error_))
{
}

AtomicAuthorityStartupStatusSnapshot::Ptr AtomicAuthorityStartupStatusSnapshot::create(
    UUID database_uuid,
    std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics,
    std::vector<AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects,
    String global_last_error,
    AtomicAuthorityStartupDependentObjectScope dependent_object_scope)
{
    if (database_uuid == UUIDHelpers::Nil)
        throw std::invalid_argument("Atomic degraded startup status has a nil database UUID");
    if (global_last_error.empty() || global_last_error.size() > atomic_authority_startup_maximum_last_error_bytes
        || containsZero(global_last_error))
        throw std::invalid_argument("Atomic degraded startup global diagnostic is invalid");
    if (dependent_object_scope != AtomicAuthorityStartupDependentObjectScope::Exact
        && dependent_object_scope != AtomicAuthorityStartupDependentObjectScope::Unknown)
        throw std::invalid_argument("Atomic degraded startup dependent-object scope is invalid");

    std::sort(
        diagnostics.begin(),
        diagnostics.end(),
        [](const auto & lhs, const auto & rhs) { return authorityInventoryKeyLess(lhs.key, rhs.key); });
    for (std::size_t index = 0; index < diagnostics.size(); ++index)
    {
        const auto & diagnostic = diagnostics[index];
        if (diagnostic.key.format_version != authority_inventory_format_version
            || diagnostic.key.record_kind != AuthorityInventoryRecordKind::TypeDefinition || diagnostic.key.object_uuid == UUIDHelpers::Nil
            || !diagnostic.revision || !isDegradedStatus(diagnostic.status) || diagnostic.last_error.empty()
            || diagnostic.last_error.size() > atomic_authority_startup_maximum_last_error_bytes || containsZero(diagnostic.last_error))
        {
            throw std::invalid_argument("Atomic degraded startup definition diagnostic is invalid");
        }
        if (index && diagnostics[index - 1].key == diagnostic.key)
            throw std::invalid_argument("Atomic degraded startup definition diagnostics contain a duplicate identity");
        if (diagnostic.record
            && (diagnostic.record->identity.database_uuid != database_uuid
                || diagnostic.record->identity.type_uuid != diagnostic.key.object_uuid
                || diagnostic.record->identity.revision != diagnostic.revision))
        {
            throw std::invalid_argument("Atomic degraded startup decoded record differs from its durable identity");
        }
    }

    if (dependent_object_scope == AtomicAuthorityStartupDependentObjectScope::Unknown && !expected_dependent_objects.empty())
        throw std::invalid_argument("Atomic degraded startup unknown scope retained partial dependent-object identities");
    std::sort(
        expected_dependent_objects.begin(),
        expected_dependent_objects.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.object_uuid < rhs.object_uuid; });
    if (std::ranges::any_of(
            expected_dependent_objects,
            [](const auto & object)
            { return object.object_uuid == UUIDHelpers::Nil || object.object_name.empty() || containsZero(object.object_name); })
        || std::adjacent_find(
               expected_dependent_objects.begin(),
               expected_dependent_objects.end(),
               [](const auto & lhs, const auto & rhs) { return lhs.object_uuid == rhs.object_uuid; })
            != expected_dependent_objects.end())
    {
        throw std::invalid_argument("Atomic degraded startup expectation identities are invalid or duplicated");
    }
    std::vector<size_t> expected_dependent_objects_by_name(expected_dependent_objects.size());
    for (size_t index = 0; index < expected_dependent_objects.size(); ++index)
        expected_dependent_objects_by_name[index] = index;
    std::sort(
        expected_dependent_objects_by_name.begin(),
        expected_dependent_objects_by_name.end(),
        [&](size_t lhs, size_t rhs) { return expected_dependent_objects[lhs].object_name < expected_dependent_objects[rhs].object_name; });
    if (std::adjacent_find(
            expected_dependent_objects_by_name.begin(),
            expected_dependent_objects_by_name.end(),
            [&](size_t lhs, size_t rhs)
            { return expected_dependent_objects[lhs].object_name == expected_dependent_objects[rhs].object_name; })
        != expected_dependent_objects_by_name.end())
    {
        throw std::invalid_argument("Atomic degraded startup expectation names are duplicated");
    }

    AuthorityDefinitionStatus global_status = AuthorityDefinitionStatus::Incomplete;
    if (std::ranges::any_of(diagnostics, [](const auto & diagnostic) { return diagnostic.status == AuthorityDefinitionStatus::Invalid; }))
        global_status = AuthorityDefinitionStatus::Invalid;
    else if (
        std::ranges::any_of(
            diagnostics, [](const auto & diagnostic) { return diagnostic.status == AuthorityDefinitionStatus::Conflicted; }))
        global_status = AuthorityDefinitionStatus::Conflicted;

    return Ptr(new AtomicAuthorityStartupStatusSnapshot(
        database_uuid,
        std::move(diagnostics),
        std::move(expected_dependent_objects),
        std::move(expected_dependent_objects_by_name),
        dependent_object_scope,
        global_status,
        std::move(global_last_error)));
}

AtomicAuthorityStartupStatusSnapshot::Ptr AtomicAuthorityStartupStatusSnapshot::createForUnavailableRoot(
    const AuthorityRoot & root,
    std::span<const AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects,
    String stable_error)
{
    /// Retain the exact durable identities without cloning every canonical
    /// definition payload.  The recovered root can be as large as the full
    /// database catalog; duplicating it solely for a failure contingency would
    /// turn every successful startup into a second catalog-sized allocation.
    /// A late mapped-object bind failure makes definitions non-executable, so
    /// UUID/revision diagnostics are sufficient until the next clean restart.
    std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics;
    diagnostics.reserve(root.getDefinitionRecordCount());
    for (const auto & record : root.getDefinitionRecords())
    {
        diagnostics.push_back({
            .key = {
                .format_version = authority_inventory_format_version,
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = record.identity.type_uuid,
            },
            .revision = record.identity.revision,
            .record = std::nullopt,
            .status = AuthorityDefinitionStatus::Incomplete,
            .last_error = stable_error,
        });
    }

    std::vector<AtomicAuthorityStartupDependentObjectIdentity> exact_objects(
        expected_dependent_objects.begin(), expected_dependent_objects.end());
    std::vector<UUID> expected_root_uuids;
    expected_root_uuids.reserve(root.getExpectationRecordCount());
    for (const auto & expectation : root.getExpectationRecords())
    {
        const auto kind = expectation.object.kind;
        if (kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary)
            expected_root_uuids.push_back(expectation.object.object_uuid);
    }
    std::sort(expected_root_uuids.begin(), expected_root_uuids.end());
    std::vector<UUID> supplied_uuids;
    supplied_uuids.reserve(exact_objects.size());
    for (const auto & object : exact_objects)
        supplied_uuids.push_back(object.object_uuid);
    std::sort(supplied_uuids.begin(), supplied_uuids.end());
    if (supplied_uuids != expected_root_uuids)
        throw std::invalid_argument("Atomic unavailable-root status does not cover the exact expectation set");

    return create(
        root.getDatabaseUUID(),
        std::move(diagnostics),
        std::move(exact_objects),
        std::move(stable_error),
        AtomicAuthorityStartupDependentObjectScope::Exact);
}

bool AtomicAuthorityStartupStatusSnapshot::containsExpectedDependentObject(UUID object_uuid) const noexcept
{
    return findExpectedDependentObject(object_uuid) != nullptr;
}

const AtomicAuthorityStartupDependentObjectIdentity *
AtomicAuthorityStartupStatusSnapshot::findExpectedDependentObject(UUID object_uuid) const noexcept
{
    if (object_uuid == UUIDHelpers::Nil)
        return nullptr;
    const auto it = std::lower_bound(
        expected_dependent_objects.begin(),
        expected_dependent_objects.end(),
        object_uuid,
        [](const auto & object, UUID uuid) { return object.object_uuid < uuid; });
    return it != expected_dependent_objects.end() && it->object_uuid == object_uuid ? &*it : nullptr;
}

const AtomicAuthorityStartupDependentObjectIdentity *
AtomicAuthorityStartupStatusSnapshot::findExpectedDependentObject(std::string_view object_name) const noexcept
{
    if (object_name.empty())
        return nullptr;
    const auto it = std::lower_bound(
        expected_dependent_objects_by_name.begin(),
        expected_dependent_objects_by_name.end(),
        object_name,
        [&](size_t index, std::string_view name) { return std::string_view(expected_dependent_objects[index].object_name) < name; });
    if (it == expected_dependent_objects_by_name.end() || std::string_view(expected_dependent_objects[*it].object_name) != object_name)
        return nullptr;
    return &expected_dependent_objects[*it];
}

}
