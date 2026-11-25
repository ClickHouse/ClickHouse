#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCommitTable.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogConstants.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCreateTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <fmt/ranges.h>

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

/// TODO: add a fuzzer for the requirements and updates of a commit request.

/// Requirements.

namespace
{

using RequirementCheck = std::function<std::expected<void, String>(const Poco::JSON::Object & metadata, const Poco::JSON::Object & requirement)>;

/// Verify table does not yet exist.
std::expected<void, String> checkCreate(const Poco::JSON::Object &, const Poco::JSON::Object &)
{
    /// `handleUpdateTable` checks requirements only against an existing table.
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
    const auto ref = getString(requirement, f_ref);
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
    {f_assert_create, checkCreate},
    {f_assert_table_uuid, checkTableUuid},
    {f_assert_ref_snapshot_id, checkRefSnapshotId},
    {f_assert_current_schema_id,
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, f_current_schema_id, f_current_schema_id); }},
    {f_assert_last_assigned_field_id,
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, f_last_assigned_field_id, f_last_column_id); }},
    {f_assert_last_assigned_partition_id,
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, f_last_assigned_partition_id, f_last_partition_id); }},
    {f_assert_default_spec_id,
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, f_default_spec_id, f_default_spec_id); }},
    {f_assert_default_sort_order_id,
     [](const auto & metadata, const auto & requirement) { return checkInteger(metadata, requirement, f_default_sort_order_id, f_default_sort_order_id); }},
};

}

bool isCreateCommit(const Poco::JSON::Array & requirements)
{
    size_t assert_create_count = 0;
    for (const auto & requirement : requirements)
    {
        if (!requirements.isObject(requirement))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every requirement must be an object");
        if (getString(*requirement.extract<Poco::JSON::Object::Ptr>(), f_type) == f_assert_create)
            ++assert_create_count;
    }
    if (assert_create_count && assert_create_count != requirements.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A create commit cannot have requirements other than 'assert-create'");
    return assert_create_count;
}

std::expected<void, String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements)
{
    for (const auto & requirement : requirements)
    {
        if (!requirements.isObject(requirement))
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
    const auto new_snapshot = getObject(update, f_snapshot);
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
    const auto ref_name = getString(update, f_ref_name);
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
    const auto ref_name = getString(update, f_ref_name);
    getObject(metadata, f_refs)->remove(ref_name);
    if (ref_name == f_main)
        metadata.set(f_current_snapshot_id, -1);
}

void removeSnapshots(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    /// Parse snapshots to remove.
    SetWithMemoryTracking<Int64> ids_to_remove;
    for (const auto & id : *getArray(update, f_snapshot_ids))
    {
        if (!id.isInteger())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every element of '{}' must be an integer", f_snapshot_ids);
        ids_to_remove.insert(id.convert<Int64>());
    }

    /// Refs keep their snapshots. A client that expires a ref removes it first, in same commit.
    for (const auto & [ref_name, ref] : *getObject(metadata, f_refs))
    {
        const auto snapshot_id = getInteger(*ref.extract<Poco::JSON::Object::Ptr>(), f_metadata_snapshot_id);
        if (ids_to_remove.contains(snapshot_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove snapshot {}: ref '{}' points to it", snapshot_id, ref_name);
    }

    /// Remove snapshots.
    VectorWithMemoryTracking<Int64> removed_ids;
    Poco::JSON::Array::Ptr kept_snapshots = new Poco::JSON::Array;
    for (const auto & snapshot : *getArray(metadata, f_snapshots))
    {
        const auto snapshot_id = getInteger(*snapshot.extract<Poco::JSON::Object::Ptr>(), f_metadata_snapshot_id);
        if (ids_to_remove.contains(snapshot_id))
            removed_ids.push_back(snapshot_id);
        else
            kept_snapshots->add(snapshot);
    }
    metadata.set(f_snapshots, kept_snapshots);

    /// Remove history up to the last removed snapshot. A gap in snapshot-log would make time travel pick a wrong snapshot.
    const auto snapshot_log = getArray(metadata, f_snapshot_log);
    Poco::JSON::Array::Ptr kept_snapshot_log = new Poco::JSON::Array;
    for (const auto & entry : *snapshot_log)
    {
        if (ids_to_remove.contains(getInteger(*entry.extract<Poco::JSON::Object::Ptr>(), f_metadata_snapshot_id)))
            kept_snapshot_log->clear();
        else
            kept_snapshot_log->add(entry);
    }
    const auto removed_log_entries = snapshot_log->size() - kept_snapshot_log->size();
    metadata.set(f_snapshot_log, kept_snapshot_log);

    /// Commit can still be rejected after this point.
    LOG_DEBUG(
        getLogger("IcebergRESTCatalogCommitTable"),
        "Removing snapshots [{}] and {} snapshot-log entries from table with uuid {}",
        fmt::join(removed_ids, ", "),
        removed_log_entries,
        getString(metadata, f_table_uuid));

    /// TODO: remove statistics files of removed snapshots, once `set-statistics` is supported.
}

void setProperties(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto updates = getObject(update, f_updates);
    auto properties = getObject(metadata, f_properties);
    for (const auto & [key, value] : *updates)
        properties->set(key, value.convert<String>());
}

void removeProperties(Poco::JSON::Object & metadata, const Poco::JSON::Object & update)
{
    const auto removals = getArray(update, f_removals);
    auto properties = getObject(metadata, f_properties);
    for (const auto & key : *removals)
        properties->remove(key.convert<String>());
}

const std::unordered_map<String, UpdateAction> UPDATE_ACTIONS = {
    {f_add_snapshot, addSnapshot},
    {f_set_snapshot_ref, setSnapshotRef},
    {f_remove_snapshot_ref, removeSnapshotRef},
    {f_remove_snapshots, removeSnapshots},
    {f_set_properties, setProperties},
    {f_remove_properties, removeProperties},
};

/// TODO: Support more updates.
const std::set<String> UNSUPPORTED_UPDATE_ACTIONS = {
    f_assign_uuid, f_upgrade_format_version, f_add_schema, f_set_current_schema, f_add_spec, f_set_default_spec,
    f_add_sort_order, f_set_default_sort_order, f_set_location, f_set_statistics, f_remove_statistics,
    f_set_partition_statistics, f_remove_partition_statistics, f_remove_partition_specs, f_remove_schemas,
    f_add_encryption_key, f_remove_encryption_key,
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
        const auto action = getString(*update_object, f_action);
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

void checkSetsMostRecent(const Poco::JSON::Object & update, const String & key)
{
    /// A create commit sends `add-schema`, then `set-current-schema` with `schema-id: -1`. `-1` means the schema added in this commit.
    /// `buildInitialTableMetadata` already makes the added schema, spec and sort order current. So `-1` is the only value this accepts.
    if (getInteger(update, key) != -1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "In a create commit, '{}' must be -1", key);
}

void setOnce(Poco::JSON::Object::Ptr & target, Poco::JSON::Object::Ptr value, const String & action)
{
    if (target)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "A create commit with more than one '{}' is not supported", action);
    target = value;
}

}

void applyTableUpdatesToJSON(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location)
{
    /// Read before the updates, which change the metadata in place.
    const auto previous_last_updated_ms = getInteger(metadata, f_last_updated_ms);
    const auto previous_current_snapshot_id = getInteger(metadata, f_current_snapshot_id);
    /// The commit time. Never earlier than the previous commit, because clocks differ between catalog servers.
    metadata.set(f_last_updated_ms, std::max(nowMs(), previous_last_updated_ms + 1));

    applyUpdates(metadata, updates);

    appendSnapshotLog(metadata, previous_current_snapshot_id);
    appendMetadataLog(metadata, current_metadata_location, previous_last_updated_ms);
}

Poco::JSON::Object::Ptr buildMetadataFromStagedCreateCommit(const Poco::JSON::Array & updates)
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
        const auto action = getString(*update_object, f_action);
        if (action == f_assign_uuid)
        {
            uuid = getString(*update_object, f_uuid);
            UUID parsed;
            if (!tryParse(parsed, uuid))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a valid uuid", uuid);
            uuid = toString(parsed);
        }
        else if (action == f_upgrade_format_version)
        {
            /// The client sends the format version it wants for the new table.
            /// `buildInitialTableMetadata` always builds version 2 metadata, so only version 2 is accepted.
            /// TODO: support format versions 1 and 3. Both need changes in `addSnapshot`.
            const auto format_version = getInteger(*update_object, f_format_version);
            if (format_version != 2)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only format-version 2 is supported, got {}", format_version);
        }
        else if (action == f_add_schema)
            setOnce(schema, getObject(*update_object, f_schema), action);
        else if (action == f_add_spec)
            setOnce(partition_spec, getObject(*update_object, f_spec), action);
        else if (action == f_add_sort_order)
            setOnce(write_order, getObject(*update_object, f_sort_order), action);
        else if (action == f_set_current_schema)
            checkSetsMostRecent(*update_object, f_schema_id);
        else if (action == f_set_default_spec)
            checkSetsMostRecent(*update_object, f_spec_id);
        else if (action == f_set_default_sort_order)
            checkSetsMostRecent(*update_object, f_update_sort_order_id);
        else if (action == f_set_location)
            location = stripTrailingSlashes(getString(*update_object, f_location));
        else
            other_updates.add(update);
    }

    if (uuid.empty() || location.empty() || !schema)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A create commit must assign a uuid, set a location and add a schema");

    /// Partition field ids start at 1000 again, as in the staged response. So the client and the catalog agree on them.
    auto metadata = buildInitialTableMetadata(uuid, location, schema, partition_spec, write_order, /*properties*/ {});
    applyUpdates(*metadata, other_updates);
    /// The commit may also write data and move `main` to a new snapshot. Record that in `snapshot-log`.
    /// Before the commit, the table had no snapshot, so the previous snapshot id is `-1`.
    appendSnapshotLog(*metadata, -1);
    return metadata;
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
