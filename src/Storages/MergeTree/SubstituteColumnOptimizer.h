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

class SubstituteColumnOptimizer
{
public:
    SubstituteColumnOptimizer(
                            ASTSelectQuery * select_query,
                            Aliases & /* aliases */, const NameSet & /* source_columns_set */,
                            const std::vector<TableWithColumnNamesAndTypes> & /* tables_with_columns */,
                            const StorageMetadataPtr & /* metadata_snapshot */,
                            const ConstStoragePtr & storage);

    void perform();

private:
    ASTSelectQuery * select_query;
    /*Aliases & aliases;
    const NameSet & source_columns_set;
    const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns;*/
    const StorageMetadataPtr & metadata_snapshot;
    ConstStoragePtr storage;
};

}
