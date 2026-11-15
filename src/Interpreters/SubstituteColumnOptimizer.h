#pragma once

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimizer that tries to replace columns to equal columns (according to constraints)
/// with lower size (according to compressed and uncompressed sizes).
class SubstituteColumnOptimizer
{
public:
    SubstituteColumnOptimizer(
        ASTSelectQuery * select_query,
        const StorageMetadataPtr & metadata_snapshot,
        const ConstStoragePtr & storage);

    void perform();

private:
    ASTSelectQuery * select_query;
    const StorageMetadataPtr & metadata_snapshot;
    ConstStoragePtr storage;
};

}
