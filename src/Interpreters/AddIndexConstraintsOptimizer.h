#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/TreeCNFConverter.h>


namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimizer that extracts constraints that
/// depends only on columns of primary key
/// and tries to add function 'indexHint' to
/// WHERE clause, which reduces amount of read data.
class AddIndexConstraintsOptimizer final
{
public:
    explicit AddIndexConstraintsOptimizer(const StorageMetadataPtr & metadata_snapshot);

    void perform(CNFQuery & cnf_query);

private:
    const StorageMetadataPtr & metadata_snapshot;
};

}
