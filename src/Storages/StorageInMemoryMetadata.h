#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/DataDestinationType.h>
#include <Core/Field.h>
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

/// Common structure for primary, partition and other storage keys
struct StorageMetadataKeyField
{
    /// User defined AST in CREATE/ALTER query. This field may be empty, but key
    /// can exists because some of them maybe set implicitly (for example,
    /// primary key in merge tree can be part of sorting key)
    ASTPtr definition_ast;

    /// ASTExpressionList with key fields, example: (x, toStartOfMonth(date))).
    ASTPtr expression_list_ast;

    /// Expression from expression_list_ast created by ExpressionAnalyzer. Useful,
    /// when you need to get required columns for key, example: a, date, b.
    ExpressionActionsPtr expression;

    /// Sample block with key columns (names, types, empty column)
    Block sample_block;

    /// Column names in key definition, example: x, toStartOfMonth(date), a * b.
    Names column_names;

    /// Types from sample block ordered in columns order.
    DataTypes data_types;

    /// Parse key structure from key definition. Requires all columns, available
    /// in storage.
    static StorageMetadataKeyField getKeyFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context);
};

/// Common struct for TTL record in storage
struct StorageMetadataTTLField
{
    /// Expression part of TTL AST:
    /// TTL d + INTERVAL 1 DAY
    ///    ^~~~~expression~~~~^
    ASTPtr expression_ast;

    /// Expresion actions evaluated from AST
    ExpressionActionsPtr expression;

    /// Result column of this TTL expression
    String result_column;

    /// Destination type, only valid for table TTLs.
    /// For example DISK or VOLUME
    DataDestinationType destination_type;

    /// Name of destination disk or volume
    String destination_name;

    /// Parse TTL structure from definition. Able to parse both column and table
    /// TTLs.
    static StorageMetadataTTLField getTTLFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context);
};

/// Mapping from column name to column TTL
using StorageMetadataTTLColumnFields = std::unordered_map<String, StorageMetadataTTLField>;
using StorageMetadataTTLFields = std::vector<StorageMetadataTTLField>;

/// Common TTL for all table. Specified after defining the table columns.
struct StorageMetadataTableTTL
{
    /// Definition. Include all parts of TTL:
    /// TTL d + INTERVAL 1 day TO VOLUME 'disk1'
    /// ^~~~~~~~~~~~~~~definition~~~~~~~~~~~~~~~^
    ASTPtr definition_ast;

    /// Rows removing TTL
    StorageMetadataTTLField rows_ttl;

    /// Moving data TTL (to other disks or volumes)
    StorageMetadataTTLFields move_ttl;
};

struct StorageMetadataSkipIndexField
{
    ASTPtr definition_ast;

    String name;

    String type;

    ExpressionActionsPtr expression;

    FieldVector arguments;

    Names column_names;

    DataTypes data_types;

    Block sample_block;

    size_t granularity;

    static StorageMetadataSkipIndexField getSkipIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context);
};

struct StorageMetadataSkipIndices
{
    std::vector<StorageMetadataSkipIndexField> indices;

    bool empty() const;
    bool has(const String & name) const;
    String toString() const;

    static StorageMetadataSkipIndices parse(const String & str, const ColumnsDescription & columns, const Context & context);
};

}
