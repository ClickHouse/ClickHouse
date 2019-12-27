#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct StorageInMemoryMetadata
{
    ColumnsDescription columns;
    IndicesDescription indices;
    ConstraintsDescription constraints;
    ASTPtr order_by_ast = nullptr;
    ASTPtr primary_key_ast = nullptr;
    ASTPtr ttl_for_table_ast = nullptr;
    ASTPtr settings_ast = nullptr;
};

}
