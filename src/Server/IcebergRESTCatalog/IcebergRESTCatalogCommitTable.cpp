#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCommitTable.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <algorithm>
#include <expected>
#include <functional>
#include <set>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int METADATA_MISMATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace IcebergRESTCatalog
{

using namespace Iceberg;

/// Requirements.

namespace
{

using RequirementCheck = std::function<std::expected<void, String>(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement)>;

/// Verify table does not yet exist.
std::expected<void, String> checkCreate(const Poco::JSON::Object &, const Poco::JSON::Object &)
{
    /// Stage-create is not supported, so the table always exists at this point.
    return std::unexpected("Table already exists");
}

/// Verify table has expected UUID.
std::expected<void, String> checkTableUuid(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement)
{
    const auto expected = getString(requirement, f_uuid);
    const auto actual = getString(metadata, f_table_uuid);
    if (expected != actual)
        return std::unexpected(fmt::format("Table uuid is {}, expected {}", actual, expected));
    return {};
}

/// Verify given ref has expected snapshot ID.
std::expected<void, String> checkRefSnapshotId(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement)
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
        return std::unexpected(fmt::format(
            "Ref '{}' points to snapshot {}, expected {}",
            ref,
            actual ? toString(*actual) : "none",
            expected ? toString(*expected) : "none"));
    return {};
}

/// Compares one integer of the requirement with one integer of the metadata.
std::expected<void, String> checkInteger(
    const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement, const String & requirement_key, const String & metadata_key)
{
    const auto expected = getInteger(requirement, requirement_key);
    const auto actual = getInteger(metadata, metadata_key);
    if (expected != actual)
        return std::unexpected(fmt::format("{} is {}, expected {}", metadata_key, actual, expected));
    return {};
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

std::expected<void, String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements)
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

        if (auto result = it->second(metadata, *requirement_object); !result)
            return result;
    }
    return {};
}

/// Updates.

namespace
{

using UpdateAction = std::function<void(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)>;

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

void validateSnapshot(const Poco::JSON::Object & snapshot)
{
    validateInteger(snapshot, f_metadata_snapshot_id);
    validateInteger(snapshot, f_metadata_sequence_number);
    validateInteger(snapshot, f_timestamp_ms);
    validateString(snapshot, f_manifest_list);
    validateOptionalInteger(snapshot, f_schema_id);
    validateOptionalInteger(snapshot, f_parent_snapshot_id);

    const auto summary = getObject(snapshot, f_summary);
    validateString(*summary, f_operation);
    for (const auto & [key, value] : *summary)
    {
        if (!value.isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Summary value '{}' must be a string", key);
    }
}

void addSnapshot(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto new_snapshot = getObject(update, "snapshot");
    validateSnapshot(*new_snapshot);

    /// Check the snapshot is indeed new.
    const auto new_snapshot_id = getInteger(*new_snapshot, f_metadata_snapshot_id);
    if (findSnapshot(metadata, new_snapshot_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Snapshot {} already exists", new_snapshot_id);

    /// A stale sequence number means another commit landed since the client read the metadata, possibly on another ref.
    /// The handler maps `METADATA_MISMATCH` to a 409 response, so the client reloads the metadata and retries.
    const auto new_sequence_number = getInteger(*new_snapshot, f_metadata_sequence_number);
    const auto last_sequence_number = getInteger(metadata, f_last_sequence_number);
    if (new_sequence_number <= last_sequence_number)
        throw Exception(
            ErrorCodes::METADATA_MISMATCH, "Snapshot sequence number {} must be greater than {}", new_sequence_number, last_sequence_number);

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

    /// Iceberg requires main to be a branch. Writers append to it.
    if (ref_name == f_main && type != f_branch)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ref 'main' must be a branch, got '{}'", type);

    /// Get new snapshot-id for ref.
    const auto snapshot_id = getInteger(update, f_metadata_snapshot_id);
    const auto snapshot = findSnapshot(metadata, snapshot_id);
    if (!snapshot)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot set ref '{}': snapshot {} does not exist", ref_name, snapshot_id);

    /// Create ref. Set type, snapshot id, and retention settings.
    Poco::JSON::Object::Ptr ref = new Poco::JSON::Object;
    ref->set(f_type, type);
    ref->set(f_metadata_snapshot_id, snapshot_id);

    /// Retention settings are positive integers. Snapshot retention applies to branches only.
    for (const auto * key : {f_ref_max_ref_age_ms, f_ref_max_snapshot_age_ms, f_ref_min_snapshots_to_keep})
    {
        const auto value = getOptionalInteger(update, key);
        if (!value)
            continue;
        if (*value <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must be greater than 0, got {}", key, *value);
        if (type == f_tag && key != f_ref_max_ref_age_ms)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not allowed on a tag", key);
        ref->set(key, *value);
    }

    /// Add ref to metadata.
    getObject(metadata, f_refs)->set(ref_name, ref);

    /// If ref is main, update current_snapshot_id.
    if (ref_name == f_main)
        metadata.set(f_current_snapshot_id, snapshot_id);
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
const std::set<String> UNSUPPORTED_UPDATE_ACTIONS = {
    "assign-uuid", "upgrade-format-version", "add-schema", "set-current-schema", "add-spec", "set-default-spec",
    "add-sort-order", "set-default-sort-order", "remove-snapshots", "set-location", "set-statistics", "remove-statistics",
    "set-partition-statistics", "remove-partition-statistics", "remove-partition-specs", "remove-schemas",
    "add-encryption-key", "remove-encryption-key",
};

/// Record the new current snapshot in snapshot-log.
void appendSnapshotLog(Poco::JSON::Object & metadata, Int64 previous_current_snapshot_id)
{
    /// One entry per commit, because readers never saw the intermediate states.
    const auto current_snapshot_id = getInteger(metadata, f_current_snapshot_id);
    if (current_snapshot_id == previous_current_snapshot_id || current_snapshot_id == -1)
        return;

    /// The log records when main moved, which is now. The snapshot itself may be old, for example on a rollback.
    Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
    entry->set(f_metadata_snapshot_id, current_snapshot_id);
    entry->set(f_timestamp_ms, getInteger(metadata, f_last_updated_ms));
    getArray(metadata, f_snapshot_log)->add(entry);
}

/// Record the previous metadata in metadata-log.
void appendMetadataLog(Poco::JSON::Object & metadata, const String & previous_metadata_location, Int64 previous_last_updated_ms)
{
    Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
    entry->set(f_metadata_file, previous_metadata_location);
    entry->set(f_timestamp_ms, previous_last_updated_ms);

    auto metadata_log = getArray(metadata, f_metadata_log);
    metadata_log->add(entry);

    const auto properties = getObject(metadata, f_properties);
    const auto max_entries = properties->optValue<size_t>("write.metadata.previous-versions-max", 100);
    size_t removed = 0;
    while (metadata_log->size() > max_entries)
    {
        metadata_log->remove(0);
        ++removed;
    }
    if (removed > 0)
        LOG_TRACE(getLogger("IcebergRESTCatalogCommitTable"), "Removed {} oldest entries from metadata-log, keeping {}", removed, max_entries);
}

}

void applyTableUpdatesToJSON(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location)
{
    /// Read before the updates, which change the metadata in place.
    const auto previous_last_updated_ms = getInteger(metadata, f_last_updated_ms);
    const auto previous_current_snapshot_id = getInteger(metadata, f_current_snapshot_id);
    /// The commit time. Never earlier than the previous commit, because clocks differ between catalog servers.
    metadata.set(f_last_updated_ms, std::max(nowMs(), previous_last_updated_ms + 1));

    for (const auto & update : updates)
    {
        /// Get update action.
        if (update.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every update must be an object");
        const auto & update_object = update.extract<Poco::JSON::Object::Ptr>();

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

    appendSnapshotLog(metadata, previous_current_snapshot_id);
    appendMetadataLog(metadata, current_metadata_location, previous_last_updated_ms);
}

String nextMetadataLocation(
    const Poco::JSON::Object & metadata, const String & current_metadata_location, const String & compression_suffix)
{
#if USE_AVRO
    const auto version = getMetadataFileAndVersion(current_metadata_location).version;
    return fmt::format(
        "{}/metadata/v{}-{}{}.metadata.json",
        getString(metadata, f_location),
        version + 1,
        toString(UUIDHelpers::generateV4()),
        compression_suffix);
#else
    UNUSED(metadata, compression_suffix);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cannot compute the next metadata location for '{}': ClickHouse was built without Avro support", current_metadata_location);
#endif
}

}

}
