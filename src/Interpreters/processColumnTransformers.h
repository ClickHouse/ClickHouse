#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Process column transformers (e.g. * EXCEPT(a)), asterisks and qualified columns.
ASTPtr processColumnTransformers(
        const String & current_database,
        const StoragePtr & table,
        const StorageMetadataPtr & metadata_snapshot,
        ASTPtr query_columns);

ASTPtr processColumnTransformers(
        const ColumnsDescription & columns,
        ASTPtr query_columns,
        const String & current_database,
        const StorageID & table_id);

}
