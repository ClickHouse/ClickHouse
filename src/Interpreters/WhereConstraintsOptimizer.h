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

class WhereConstraintsOptimizer final
{
public:
    WhereConstraintsOptimizer(
        ASTSelectQuery * select_query,
        const StorageMetadataPtr & metadata_snapshot,
        const bool optimize_append_index,
        const bool optimize_use_smt);

    void perform();

private:
    ASTSelectQuery * select_query;
    const StorageMetadataPtr & metadata_snapshot;
    bool optimize_append_index;
    bool optimize_use_smt;
};

}
