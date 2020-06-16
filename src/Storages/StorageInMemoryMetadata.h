#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/TTLDescription.h>
#include <Storages/SelectQueryDescription.h>


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
    IndicesDescription secondary_indices;
    /// Table constraints. Currently supported for MergeTree only.
    ConstraintsDescription constraints;
    /// PARTITION BY expression. Currently supported for MergeTree only.
    KeyDescription partition_key;
    /// PRIMARY KEY expression. If absent, than equal to order_by_ast.
    KeyDescription primary_key;
    /// ORDER BY expression. Required field for all MergeTree tables
    /// even in old syntax MergeTree(partition_key, order_by, ...)
    KeyDescription sorting_key;
    /// SAMPLE BY expression. Supported for MergeTree only.
    KeyDescription sampling_key;
    /// Separate ttl expressions for columns
    TTLColumnsDescription column_ttls_by_name;
    /// TTL expressions for table (Move and Rows)
    TTLTableDescription table_ttl;
    /// SETTINGS expression. Supported for MergeTree, Buffer and Kafka.
    ASTPtr settings_changes;
    /// SELECT QUERY. Supported for MaterializedView and View (have to support LiveView).
    SelectQueryDescription select;

    StorageInMemoryMetadata() = default;
    StorageInMemoryMetadata(const ColumnsDescription & columns_, const IndicesDescription & secondary_indices_, const ConstraintsDescription & constraints_);

    StorageInMemoryMetadata(const StorageInMemoryMetadata & other);
    StorageInMemoryMetadata & operator=(const StorageInMemoryMetadata & other);
};

}
