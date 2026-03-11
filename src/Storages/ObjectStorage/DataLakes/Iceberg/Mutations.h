#pragma once

#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>
#include "config.h"

#if USE_AVRO

#include <Databases/DataLake/ICatalog.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB::Iceberg
{

void mutate(
    const MutationCommands & commands,
    ContextPtr context,
    StorageMetadataPtr storage_metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_table_components,
    const String & write_format,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & blob_storage_type_name,
    const String & blob_storage_namespace_name);

void alter(
    const AlterCommands & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_table_components,
    const String & write_format);

struct ExpireSnapshotsResult
{
    Int64 deleted_data_files_count = 0;
    Int64 deleted_position_delete_files_count = 0;
    Int64 deleted_equality_delete_files_count = 0;
    Int64 deleted_manifest_files_count = 0;
    Int64 deleted_manifest_lists_count = 0;
    Int64 deleted_statistics_files_count = 0;
};

ExpireSnapshotsResult expireSnapshots(
    std::optional<Int64> expire_before_ms,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    PersistentTableComponents & persistent_table_components,
    const String & write_format,
    std::shared_ptr<DataLake::ICatalog> catalog,
    const String & blob_storage_type_name,
    const String & blob_storage_namespace_name,
    const String & table_name);
}

#endif
