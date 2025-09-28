#pragma once

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
    StorageMetadataPtr metadata,
    StorageID storage_id,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings,
    std::shared_ptr<DataLake::ICatalog> catalog);

void alter(
    const AlterCommands & params,
    ContextPtr context,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration
);

}

#endif
