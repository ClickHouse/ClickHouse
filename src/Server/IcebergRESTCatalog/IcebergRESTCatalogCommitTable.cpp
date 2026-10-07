#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCommitTable.h>

#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCreateTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <set>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

using namespace Iceberg;
using namespace IcebergRESTCatalogJSON;

/// Requirements.

namespace
{

using RequirementCheck = std::optional<String> (*)(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement);

/// Verify table does not exist.
std::optional<String> checkCreate(const Poco::JSON::Object &, const Poco::JSON::Object &)
{
    /// The table exists at this point. A commit to a missing table goes through `checkCreateRequirements`.
    return "Table already exists";
}

/// Verify table has expected UUID.
std::optional<String> checkTableUuid(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement)
{
    const auto expected = getString(requirement, f_uuid);
    const auto actual = getString(metadata, f_table_uuid);
    if (expected != actual)
        return fmt::format("Table uuid is {}, expected {}", actual, expected);
    return std::nullopt;
}

/// Verify given ref has expected snapshot ID.
std::optional<String> checkRefSnapshotId(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement)
{
    /// Get expected ref. Missing or null means it expects no existing ref.
    const auto ref = getString(requirement, "ref");
    const auto expected = getOptionalInteger(requirement, f_metadata_snapshot_id);

    /// Get actual ref, if exists.
    std::optional<Int64> actual;
    const auto refs = metadata.getObject(f_refs);
    if (refs && refs->has(ref))
        actual = getInteger(*getObject(*refs, ref), f_metadata_snapshot_id);

    /// Compare.
    if (expected != actual)
        return fmt::format(
            "Ref '{}' points to snapshot {}, expected {}",
            ref,
            actual ? toString(*actual) : "none",
            expected ? toString(*expected) : "none");
    return std::nullopt;
}

/// Compares one integer of the requirement with one integer of the metadata.
std::optional<String> checkInteger(
    const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement, const String & requirement_key, const String & metadata_key)
{
    const auto expected = getInteger(requirement, requirement_key);
    const auto actual = getInteger(metadata, metadata_key);
    if (expected != actual)
        return fmt::format("{} is {}, expected {}", metadata_key, actual, expected);
    return std::nullopt;
}

const std::unordered_map<String, RequirementCheck> REQUIREMENT_CHECKS = {
    {"assert-create", checkCreate},
    {"assert-table-uuid", checkTableUuid},
    {"assert-ref-snapshot-id", checkRefSnapshotId},
    {"assert-current-schema-id",
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, "current-schema-id", f_current_schema_id); }},
    {"assert-last-assigned-field-id",
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, "last-assigned-field-id", f_last_column_id); }},
    {"assert-last-assigned-partition-id",
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, "last-assigned-partition-id", f_last_partition_id); }},
    {"assert-default-spec-id",
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, "default-spec-id", f_default_spec_id); }},
    {"assert-default-sort-order-id",
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, "default-sort-order-id", f_default_sort_order_id); }},
};

}

bool hasAssertCreate(const Poco::JSON::Array & requirements)
{
    for (const auto & requirement : requirements)
    {
        if (requirement.type() == typeid(Poco::JSON::Object::Ptr)
            && requirement.extract<Poco::JSON::Object::Ptr>()->optValue<String>(f_type, "") == "assert-create")
            return true;
    }
    return false;
}

std::optional<String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements)
{
    for (const auto & requirement : requirements)
    {
        if (requirement.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every requirement must be an object");
        const auto & requirement_object = requirement.extract<Poco::JSON::Object::Ptr>();

        const auto type = getString(*requirement_object, f_type);
        const auto it = REQUIREMENT_CHECKS.find(type);
        if (it == REQUIREMENT_CHECKS.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown requirement type '{}'", type);

        if (auto failed = it->second(metadata, *requirement_object))
            return failed;
    }
    return std::nullopt;
}

std::optional<String> checkCreateRequirements(const Poco::JSON::Array & requirements)
{
    for (const auto & requirement : requirements)
    {
        if (requirement.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every requirement must be an object");
        const auto & requirement_object = requirement.extract<Poco::JSON::Object::Ptr>();

        const auto type = getString(*requirement_object, f_type);
        if (type == "assert-create")
            continue;
        if (!REQUIREMENT_CHECKS.contains(type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown requirement type '{}'", type);
        return fmt::format("Cannot check '{}': table does not exist", type);
    }
    return std::nullopt;
}

/// Updates.

namespace
{

using UpdateAction = void (*)(Poco::JSON::Object & metadata, const Poco::JSON::Object & update);

Poco::JSON::Object::Ptr findSnapshot(const Poco::JSON::Object & metadata, Int64 snapshot_id)
{
    for (const auto & snapshot : *getArray(metadata, f_snapshots))
    {
        const auto & snapshot_object = snapshot.extract<Poco::JSON::Object::Ptr>();
        if (getInteger(*snapshot_object, f_metadata_snapshot_id) == snapshot_id)
            return snapshot_object;
    }
    return nullptr;
}

void addSnapshot(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto new_snapshot = getObject(update, "snapshot");

    /// Check required fields exist.
    validateInteger(*new_snapshot, f_timestamp_ms);
    validateString(*new_snapshot, f_manifest_list);
    validateString(*getObject(*new_snapshot, f_summary), f_operation);

    /// Check the snapshot is indeed new.
    const auto new_snapshot_id = getInteger(*new_snapshot, f_metadata_snapshot_id);
    if (findSnapshot(metadata, new_snapshot_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Snapshot {} already exists", new_snapshot_id);

    /// Check the sequence number is valid.
    const auto new_sequence_number = getInteger(*new_snapshot, f_metadata_sequence_number);
    const auto last_sequence_number = getInteger(metadata, f_last_sequence_number);
    if (new_sequence_number <= last_sequence_number)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Snapshot sequence number {} must be greater than {}", new_sequence_number, last_sequence_number);

    /// Add snapshot.
    getArray(metadata, f_snapshots)->add(new_snapshot);
    metadata.set(f_last_sequence_number, new_sequence_number);
}

void setSnapshotRef(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    /// Get ref to update.
    const auto ref_name = getString(update, "ref-name");
    const auto type = getString(update, f_type);
    if (type != f_branch && type != f_tag)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ref type must be 'branch' or 'tag', got '{}'", type);

    /// Get new snapshot-id for ref.
    const auto snapshot_id = getInteger(update, f_metadata_snapshot_id);
    const auto snapshot = findSnapshot(metadata, snapshot_id);
    if (!snapshot)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot set ref '{}': snapshot {} does not exist", ref_name, snapshot_id);

    /// Create ref. Set type, snapshot id, and retention settings.
    Poco::JSON::Object::Ptr ref = new Poco::JSON::Object;
    ref->set(f_type, type);
    ref->set(f_metadata_snapshot_id, snapshot_id);
    for (const auto * key : {f_ref_max_ref_age_ms, f_ref_max_snapshot_age_ms, f_ref_min_snapshots_to_keep})
        if (update.has(key) && !update.isNull(key))
            ref->set(key, update.get(key));

    /// Add ref to metadata.
    getObject(metadata, f_refs)->set(ref_name, ref);

    /// If ref is main, update current_snapshot_id and snapshot_log.
    if (ref_name == f_main)
    {
        metadata.set(f_current_snapshot_id, snapshot_id);

        Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
        entry->set(f_metadata_snapshot_id, snapshot_id);
        entry->set(f_timestamp_ms, snapshot->get(f_timestamp_ms));
        getArray(metadata, f_snapshot_log)->add(entry);
    }
}

void removeSnapshotRef(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto ref_name = getString(update, "ref-name");
    getObject(metadata, f_refs)->remove(ref_name);
    if (ref_name == f_main)
        metadata.set(f_current_snapshot_id, -1);
}

void setProperties(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto updates = getObject(update, "updates");
    auto properties = getObject(metadata, f_properties);
    for (const auto & [key, value] : *updates)
        properties->set(key, value.convert<String>());
}

void removeProperties(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto removals = getArray(update, "removals");
    auto properties = getObject(metadata, f_properties);
    for (const auto & key : *removals)
        properties->remove(key.convert<String>());
}

const std::unordered_map<String, UpdateAction> UPDATE_ACTIONS = {
    {"add-snapshot", addSnapshot},
    {"set-snapshot-ref", setSnapshotRef},
    {"remove-snapshot-ref", removeSnapshotRef},
    {"set-properties", setProperties},
    {"remove-properties", removeProperties},
};

/// TODO: Support more updates.
/// The structural ones (`add-schema`, etc.) are supported only in a create commit.
const std::set<String> UNSUPPORTED_UPDATE_ACTIONS = {
    "assign-uuid", "upgrade-format-version", "add-schema", "set-current-schema", "add-spec", "set-default-spec",
    "add-sort-order", "set-default-sort-order", "remove-snapshots", "set-location", "set-statistics", "remove-statistics",
    "set-partition-statistics", "remove-partition-statistics", "remove-partition-specs", "remove-schemas",
    "add-encryption-key", "remove-encryption-key",
};

/// Record the previous metadata in metadata-log.
void appendMetadataLog(Poco::JSON::Object & metadata, const String & previous_metadata_location, Int64 previous_last_updated_ms)
{
    Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
    entry->set(f_metadata_file, previous_metadata_location);
    entry->set(f_timestamp_ms, previous_last_updated_ms);

    auto log = getArray(metadata, f_metadata_log);
    log->add(entry);

    const auto properties = getObject(metadata, f_properties);
    const auto max_entries = properties->optValue<size_t>("write.metadata.previous-versions-max", 100);
    while (log->size() > max_entries)
        log->remove(0);
}

Poco::JSON::Object::Ptr getUpdateObject(const Poco::Dynamic::Var & update)
{
    if (update.type() != typeid(Poco::JSON::Object::Ptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every update must be an object");
    return update.extract<Poco::JSON::Object::Ptr>();
}

void applyUpdates(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates)
{
    for (const auto & update : updates)
    {
        const auto update_object = getUpdateObject(update);

        /// Validate action is supported.
        const auto action = getString(*update_object, "action");
        const auto it = UPDATE_ACTIONS.find(action);
        if (it == UPDATE_ACTIONS.end())
        {
            if (UNSUPPORTED_UPDATE_ACTIONS.contains(action))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Update '{}' is not supported", action);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown update action '{}'", action);
        }

        /// Apply update.
        it->second(metadata, *update_object);
    }
}

/// A create commit adds one schema, spec and sort order. So `-1` ("the last added") is the only id a `set-*` update needs.
void checkLastAdded(const Poco::JSON::Object & update, const String & key)
{
    if (getInteger(update, key) != -1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "In a create commit, '{}' must be -1", key);
}

void addOnce(Poco::JSON::Object::Ptr & target, Poco::JSON::Object::Ptr value, const String & action)
{
    if (target)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "A create commit with more than one '{}' is not supported", action);
    target = std::move(value);
}

}

void applyTableUpdates(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location)
{
    /// Read before the updates, which change the metadata in place.
    const auto previous_last_updated_ms = getInteger(metadata, f_last_updated_ms);

    applyUpdates(metadata, updates);

    appendMetadataLog(metadata, current_metadata_location, previous_last_updated_ms);
    metadata.set(f_last_updated_ms, nowMs());
}

Poco::JSON::Object::Ptr buildCreatedTableMetadata(const Poco::JSON::Array & updates)
{
    String uuid;
    String location;
    Poco::JSON::Object::Ptr schema;
    Poco::JSON::Object::Ptr partition_spec;
    Poco::JSON::Object::Ptr write_order;
    Poco::JSON::Array other_updates;

    /// Collect the updates that describe the table.
    for (const auto & update : updates)
    {
        const auto update_object = getUpdateObject(update);
        const auto action = getString(*update_object, "action");
        if (action == "assign-uuid")
        {
            uuid = getString(*update_object, f_uuid);
            UUID parsed;
            if (!tryParse(parsed, uuid))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a valid uuid", uuid);
        }
        else if (action == "upgrade-format-version")
        {
            /// TODO: support format version 3.
            const auto format_version = getInteger(*update_object, f_format_version);
            if (format_version != 2)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only format-version 2 is supported, got {}", format_version);
        }
        else if (action == "add-schema")
            addOnce(schema, getObject(*update_object, "schema"), action);
        else if (action == "add-spec")
            addOnce(partition_spec, getObject(*update_object, "spec"), action);
        else if (action == "add-sort-order")
            addOnce(write_order, getObject(*update_object, "sort-order"), action);
        else if (action == "set-current-schema")
            checkLastAdded(*update_object, f_schema_id);
        else if (action == "set-default-spec")
            checkLastAdded(*update_object, f_spec_id);
        else if (action == "set-default-sort-order")
            checkLastAdded(*update_object, "sort-order-id");
        else if (action == "set-location")
            location = stripTrailingSlashes(getString(*update_object, f_location));
        else
            other_updates.add(update);
    }

    if (uuid.empty() || location.empty() || !schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A create commit must assign a uuid, set a location and add a schema");

    /// Reassigns partition field ids from 1000, like the staged response did. So they match the ones the client got.
    auto metadata = buildInitialTableMetadata(uuid, location, schema, partition_spec, write_order, /*properties*/ {});
    applyUpdates(*metadata, other_updates);
    return metadata;
}

String nextMetadataLocation(
    const Poco::JSON::Object & metadata, const String & current_metadata_location, const String & compression_suffix)
{
    const auto version = getMetadataFileAndVersion(current_metadata_location).version;
    return fmt::format(
        "{}/metadata/v{}-{}{}.metadata.json",
        getString(metadata, f_location),
        version + 1,
        toString(UUIDHelpers::generateV4()),
        compression_suffix);
}

}
