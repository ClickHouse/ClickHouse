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
    ASTPtr order_by_expression = nullptr;
    ASTPtr primary_key_expression = nullptr;
    ASTPtr ttl_for_table_expression = nullptr;
    ASTPtr settings_ast = nullptr;
};

}
