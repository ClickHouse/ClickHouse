#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnDependency.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/TTLDescription.h>

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


    ////////////////////////////////////////////////////////////////////////
    void setColumns(ColumnsDescription columns_); /// sets only real columns, possibly overwrites virtual ones.

    void setSecondaryIndices(IndicesDescription secondary_indices_);

    void setConstraints(ConstraintsDescription constraints_);

    /// Set partition key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setPartitionKey(const KeyDescription & partition_key_);
    /// Set sorting key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setSortingKey(const KeyDescription & sorting_key_);
    /// Set primary key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setPrimaryKey(const KeyDescription & primary_key_);
    /// Set sampling key for storage (methods bellow, are just wrappers for this
    /// struct).
    void setSamplingKey(const KeyDescription & sampling_key_);

    void setTableTTLs(const TTLTableDescription & table_ttl_);

    void setColumnTTLs(const TTLColumnsDescription & column_ttls_by_name_);

    void setSettingsChanges(const ASTPtr & settings_changes_);

    void setSelectQuery(const SelectQueryDescription & select_);

    const ColumnsDescription & getColumns() const; /// returns combined set of columns
    const IndicesDescription & getSecondaryIndices() const;
    /// Has at least one non primary index
    bool hasSecondaryIndices() const;

    const ConstraintsDescription & getConstraints() const;

    /// Common tables TTLs (for rows and moves).
    TTLTableDescription getTableTTLs() const;
    bool hasAnyTableTTL() const;

    /// Separate TTLs for columns.
    TTLColumnsDescription getColumnTTLs() const;
    bool hasAnyColumnTTL() const;

    /// Just wrapper for table TTLs, return rows part of table TTLs.
    TTLDescription getRowsTTL() const;
    bool hasRowsTTL() const;

    /// Just wrapper for table TTLs, return moves (to disks or volumes) parts of
    /// table TTL.
    TTLDescriptions getMoveTTLs() const;
    bool hasAnyMoveTTL() const;

    /// Returns columns, which will be needed to calculate dependencies (skip
    /// indices, TTL expressions) if we update @updated_columns set of columns.
    ColumnDependencies getColumnDependencies(const NameSet & updated_columns) const;

    Block getSampleBlock() const; /// ordinary + materialized.
    Block getSampleBlockNonMaterialized() const; /// ordinary.
    Block getSampleBlockWithVirtuals(const NamesAndTypesList & virtuals) const; /// ordinary + materialized + virtuals.
    Block getSampleBlockForColumns(
        const Names & column_names, const NamesAndTypesList & virtuals) const; /// ordinary + materialized + aliases + virtuals.

    /// Returns structure with partition key.
    const KeyDescription & getPartitionKey() const;
    /// Returns ASTExpressionList of partition key expression for storage or nullptr if there is none.
    ASTPtr getPartitionKeyAST() const { return partition_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) partition key.
    bool isPartitionKeyDefined() const;
    /// Storage has partition key.
    bool hasPartitionKey() const;
    /// Returns column names that need to be read to calculate partition key.
    Names getColumnsRequiredForPartitionKey() const;

    /// Returns structure with sorting key.
    const KeyDescription & getSortingKey() const;
    /// Returns ASTExpressionList of sorting key expression for storage or nullptr if there is none.
    ASTPtr getSortingKeyAST() const { return sorting_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sorting key.
    bool isSortingKeyDefined() const;
    /// Storage has sorting key. It means, that it contains at least one column.
    bool hasSortingKey() const;
    /// Returns column names that need to be read to calculate sorting key.
    Names getColumnsRequiredForSortingKey() const;
    /// Returns columns names in sorting key specified by user in ORDER BY
    /// expression. For example: 'a', 'x * y', 'toStartOfMonth(date)', etc.
    Names getSortingKeyColumns() const;

    /// Returns column names that need to be read for FINAL to work.
    Names getColumnsRequiredForFinal() const { return getColumnsRequiredForSortingKey(); }

    /// Returns structure with sampling key.
    const KeyDescription & getSamplingKey() const;
    /// Returns sampling expression AST for storage or nullptr if there is none.
    ASTPtr getSamplingKeyAST() const { return sampling_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sampling key.
    bool isSamplingKeyDefined() const;
    /// Storage has sampling key.
    bool hasSamplingKey() const;
    /// Returns column names that need to be read to calculate sampling key.
    Names getColumnsRequiredForSampling() const;
};

using StorageMetadataPtr = std::shared_ptr<StorageInMemoryMetadata>;
using MultiVersionStorageMetadataPtr = MultiVersion<StorageInMemoryMetadata>;

}
