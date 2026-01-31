#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/FormatFactorySettings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTSetQuery;
struct DataLakeStorageSettingsImpl;
struct MutableColumnsAndConstraints;
class StorageObjectStorage;
class SettingsChanges;

/// List of available types supported in DataLakeStorageSettingsSettings object
#define STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, IdentifierQuotingRule) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt32) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI) \
    M(CLASS_NAME, DatabaseDataLakeCatalogType)

// clang-format off

#define DATA_LAKE_STORAGE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, iceberg_metadata_file_path, "", R"(
Explicit path to desired Iceberg metadata file, should be relative to path in object storage. Make sense for table function use case only.
)", 0) \
    DECLARE(String, iceberg_metadata_table_uuid, "", R"(
Explicit table UUID to read metadata for. Ignored if iceberg_metadata_file_path is set.
)", 0) \
    DECLARE(Bool, iceberg_recent_metadata_file_by_last_updated_ms_field, false, R"(
If enabled, the engine would use the metadata file with the most recent last_updated_ms json field. Does not make sense to use with iceberg_metadata_file_path.
)", 0) \
    DECLARE(Bool, iceberg_use_version_hint, false, R"(
Get latest metadata path from version-hint.text file.
)", 0) \
    DECLARE(NonZeroUInt64, iceberg_format_version, 2, R"(
Metadata format version.
)", 0) \
    DECLARE(Bool, paimon_incremental_read, false, R"(
Enable incremental read mode for Paimon tables. When enabled, the table will track the last committed snapshot
in Keeper and only read new data since that snapshot. This is similar to Kafka streaming consumption.
)", 0) \
    DECLARE(Int64, paimon_target_snapshot_id, -1, R"(
Session-level targeted snapshot read for Paimon incremental mode. When >0, the reader will only fetch the delta
for the specified snapshot_id without advancing the committed watermark. Useful for backfill/compensation reads.
Default: -1 (disabled)
)", 0) \
    DECLARE(Int64, paimon_metadata_refresh_interval_ms, 0, R"(
Background metadata refresh interval for Paimon tables (milliseconds).
0 disables background refresh. When >0, a background task periodically calls
metadata update to pull latest snapshot/schema. Queries still trigger update
as usual. Use cautiously on many tables to avoid excessive object storage/Keeper I/O.
Default: 0 (disabled)
)", 0) \
    DECLARE(String, paimon_keeper_path, "", R"(
Keeper path for Paimon incremental read state. Must be unique per table.
If empty, incremental read is not allowed.
)", 0) \
    DECLARE(String, paimon_replica_name, "", R"(
Replica name for Paimon incremental read state. Must be set and unique per replica.
)", 0) \
    DECLARE(DatabaseDataLakeCatalogType, storage_catalog_type, DatabaseDataLakeCatalogType::NONE, "Catalog type", 0) \
    DECLARE(String, storage_catalog_credential, "", "", 0)             \
    DECLARE(String, storage_auth_scope, "PRINCIPAL_ROLE:ALL", "Authorization scope for client credentials or token exchange", 0)             \
    DECLARE(String, storage_oauth_server_uri, "", "OAuth server uri", 0)             \
    DECLARE(Bool, storage_oauth_server_use_request_body, true, "Put parameters into request body or query params", 0)             \
    DECLARE(String, storage_warehouse, "", "Warehouse name inside the catalog", 0)             \
    DECLARE(String, storage_auth_header, "", "Authorization header of format 'Authorization: <scheme> <auth_info>'", 0)           \
    DECLARE(String, storage_aws_access_key_id, "", "Key for AWS connection for Glue catalog", 0)           \
    DECLARE(String, storage_aws_secret_access_key, "", "Key for AWS connection for Glue Catalog'", 0)           \
    DECLARE(String, storage_region, "", "Region for Glue catalog", 0)           \
    DECLARE(String, object_storage_endpoint, "", "Object storage endpoint", 0) \
    DECLARE(String, storage_catalog_url, "", "Catalog url", 0) \
    DECLARE(String, disk, "", "Disk name to use for underlying storage", 0) \

#define OBSOLETE_SETTINGS(M, ALIAS) \
    MAKE_OBSOLETE(M, Bool, allow_experimental_delta_kernel_rs, true) \
    MAKE_OBSOLETE(M, Bool, delta_lake_read_schema_same_as_table_schema, false) \
    MAKE_OBSOLETE(M, Bool, allow_dynamic_metadata_for_data_lakes, true)
// clang-format on

STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES(DataLakeStorageSettings, DECLARE_SETTING_TRAIT)

struct DataLakeStorageSettings
{
    DataLakeStorageSettings();
    DataLakeStorageSettings(const DataLakeStorageSettings & settings);
    DataLakeStorageSettings(DataLakeStorageSettings && settings) noexcept;
    ~DataLakeStorageSettings();

    STORAGE_DATA_LAKE_STORAGE_SETTINGS_SUPPORTED_TYPES(DataLakeStorageSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTSetQuery & settings_ast);

    void loadFromSettingsChanges(const SettingsChanges & changes);

    Field get(const std::string & name);

    static bool hasBuiltin(std::string_view name);

    void serialize(WriteBuffer & out) const;
    static DataLakeStorageSettings deserialize(ReadBuffer & in);

private:
    std::unique_ptr<DataLakeStorageSettingsImpl> impl;
};

using DataLakeStorageSettingsPtr = std::shared_ptr<DataLakeStorageSettings>;

#define LIST_OF_DATA_LAKE_STORAGE_SETTINGS(M, ALIAS) \
    DATA_LAKE_STORAGE_RELATED_SETTINGS(M, ALIAS) \
    OBSOLETE_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

}
