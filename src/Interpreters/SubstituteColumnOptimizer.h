#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimizer that tries to replace columns to equal columns (according to constraints)
/// with lower size (accorsing to compressed and uncomressed size).
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
