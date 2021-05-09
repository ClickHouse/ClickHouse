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

class AddIndexConstraintsOptimizer final
{
public:
    AddIndexConstraintsOptimizer(
        const StorageMetadataPtr & metadata_snapshot,
        const bool optimize_use_smt);

    void perform(CNFQuery & cnf_query);

private:
    const StorageMetadataPtr & metadata_snapshot;
    const bool optimize_use_smt;
};

}
