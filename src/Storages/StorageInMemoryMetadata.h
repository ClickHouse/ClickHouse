#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Structure represent table metadata stored in memory.
/// Only one storage engine support all fields -- MergeTree.
/// Complete table AST can be recreated from this struct.
struct StorageInMemoryMetadata
{
    /// Columns of table with their names, types,
    /// defaults, comments, etc. All table engines have columns.
    ColumnsDescription columns;
    /// Table indices. Currently supported for MergeTree only.
    IndicesDescription indices;
    /// Table constraints. Currently supported for MergeTree only.
    ConstraintsDescription constraints;
    /// PARTITION BY expression. Currently supported for MergeTree only.
    ASTPtr partition_by_ast = nullptr;
    /// ORDER BY expression. Required field for all MergeTree tables
    /// even in old syntax MergeTree(partition_key, order_by, ...)
    ASTPtr order_by_ast = nullptr;
    /// PRIMARY KEY expression. If absent, than equal to order_by_ast.
    ASTPtr primary_key_ast = nullptr;
    /// TTL expression for whole table. Supported for MergeTree only.
    ASTPtr ttl_for_table_ast = nullptr;
    /// SAMPLE BY expression. Supported for MergeTree only.
    ASTPtr sample_by_ast = nullptr;
    /// SETTINGS expression. Supported for MergeTree, Buffer and Kafka.
    ASTPtr settings_ast = nullptr;
    /// SELECT QUERY. Supported for MaterializedView only.
    ASTPtr select = nullptr;

    StorageInMemoryMetadata(const StorageInMemoryMetadata & other);
    StorageInMemoryMetadata() = default;
    StorageInMemoryMetadata(const ColumnsDescription & columns_, const IndicesDescription & indices_, const ConstraintsDescription & constraints_);

    StorageInMemoryMetadata & operator=(const StorageInMemoryMetadata & other);
};

struct StorageMetadataField
{
    ASTPtr ast;
    ExpressionActionsPtr actions;
};

struct StorageMetadataKeyField
{
    ASTPtr definition_ast;

    ASTPtr expression_ast;
    ExpressionActionsPtr expressions;
    Names expression_columns;

    Block sample_block;
    DataTypes data_types;

    StorageMetadataKeyField & operator=(const StorageMetadataKeyField & other) = default;
};

}
