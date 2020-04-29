#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Common/MultiVersion.h>

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

    const ColumnsDescription & getColumns() const; /// returns combined set of columns
    void setColumns(ColumnsDescription columns_); /// sets only real columns, possibly overwrites virtual ones.

    Block getSampleBlock() const; /// ordinary + materialized.
    Block getSampleBlockWithVirtuals(const NamesAndTypesList & virtuals) const; /// ordinary + materialized + virtuals.
    Block getSampleBlockNonMaterialized() const; /// ordinary.
    Block getSampleBlockForColumns(const Names & column_names, const NamesAndTypesList & virtuals) const; /// ordinary + materialized + aliases + virtuals.
    /// Verify that all the requested names are in the table and are set correctly:
    /// list of names is not empty and the names do not repeat.
    void check(const Names & column_names, const NamesAndTypesList & virtuals) const;

    /// Check that all the requested names are in the table and have the correct types.
    void check(const NamesAndTypesList & columns) const;

    /// Check that all names from the intersection of `names` and `columns` are in the table and have the same types.
    void check(const NamesAndTypesList & columns, const Names & column_names) const;

    /// Check that the data block contains all the columns of the table with the correct types,
    /// contains only the columns of the table, and all the columns are different.
    /// If |need_all| is set, then checks that all the columns of the table are in the block.
    void check(const Block & block, bool need_all = false) const;
    /// Returns whether the column is virtual - by default all columns are real.
    /// Initially reserved virtual column name may be shadowed by real column.
    bool isVirtualColumn(const String & column_name) const;
};

using MultiVersionStorageMetadata = MultiVersion<StorageInMemoryMetadata>;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

}
