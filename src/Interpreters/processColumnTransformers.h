#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

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

}
