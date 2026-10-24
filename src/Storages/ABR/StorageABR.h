#pragma once

#include <optional>

#include <Common/AtomicLogger.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageFactory.h>

namespace DB
{

/// Read-only proxy engine that routes queries to the best underlying MergeTree table.
///
/// Given table_prefix='logs' and sample_intervals=[1,10,100], ABR discovers
/// tables logs_1, logs_10, logs_100 and picks the finest-grained one whose
/// retention covers the query's time range. On TOO_MANY_ROWS or TIMEOUT_EXCEEDED
/// the query is retried on the next coarser table.
///
///   CREATE TABLE t ENGINE = ABR('logs', [1,10,100], ('ts','region'))
///
/// The retention_expression lists partition-key columns that ABR uses to decide
/// whether a table has enough data for a query. Its first element is the time
/// column used for time-range filtering. Before comparing dates, ABR walks each
/// table's parts and prunes those whose minmax index doesn't match the query's
/// filters on the retention columns. Only surviving parts count toward the
/// table's effective min date.
class StorageABR final : public IStorage
{
public:
    /// A query attempt against one of the underlying tables: an interpreter that is ready to
    /// execute and the header of the result it produces.
    struct Attempt
    {
        std::shared_ptr<IInterpreter> interpreter;
        SharedHeader header;
    };

    /// Discovers underlying tables {prefix}_{interval}, validates schema consistency,
    /// copies metadata from the first table, and builds retention pruning metadata.
    StorageABR(
        const StorageID & table_id,
        const String & table_prefix,
        const std::vector<UInt64> & sample_intervals_,
        const String & time_column_,
        const Names & retention_expression,
        ContextPtr context);

    /// Builds the query to run against `target`, retargeting the original query to it.
    /// With the analyzer the table expression of the query tree is replaced, otherwise the AST is rewritten.
    static ASTPtr rewriteQueryForStorage(const SelectQueryInfo & query_info, const StoragePtr & target, const ContextPtr & context);

    /// Creates a child context with the query's `SETTINGS` applied, the remaining execution time split across the
    /// `tables_left` remaining candidate tables, and the read limits split across all `total_tables` candidate tables.
    static ContextMutablePtr buildAttemptContext(
        const ContextPtr &, const SelectQueryInfo & query_info, size_t tables_left, size_t total_tables);

    /// Builds an interpreter for the query retargeted to `target`.
    /// The interpreter is not executed; the result header is computed eagerly.
    static Attempt buildAttempt(
        const SelectQueryInfo & query_info,
        const StoragePtr & target,
        const ContextPtr & context,
        size_t tables_left,
        size_t total_tables,
        QueryProcessingStage::Enum processed_stage);

    /// Returns true for error codes that trigger a retry on the next coarser table.
    static bool isFallbackError(int error_code);

    String getName() const override { return "ABR"; }

    /// Because of the retry mechanism, ABR fully executes the query before returning
    /// getQueryProcessingStage() returns to_stage unchanged and ABR hands back mergeable state
    /// This flag lets the merge machinery treat that state correctly without marking it remote
    bool producesMergeableState(const ContextPtr & /*query_context*/) const override { return true; }

    /// The query is rewritten for the chosen underlying table, see `rewriteQueryForStorage`.
    bool readRequiresAnalyzedQuery() const override { return true; }

    ColumnSizeByName getColumnSizes() const override;
    bool supportsPrewhere() const override;
    bool canMoveConditionsToPrewhere() const override;
    bool supportsFinal() const override;
    bool supportsSubcolumns() const override;

    /// Selects the best table via chooseBestTable(), then delegates to ReadFromABR
    /// which executes the query and handles retry fallback.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    /// Parses ENGINE = ABR(table_prefix, sample_intervals, retention_expression).
    ///   table_prefix:          String          — common prefix of underlying tables
    ///   sample_intervals:      Array(UInt64)   — granularity levels (sorted ascending)
    ///   retention_expression:  Tuple or String — partition columns used for part pruning;
    ///                                            first element is the time column
    static std::shared_ptr<StorageABR> creatorFunction(const StorageFactory::Arguments & args);

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr &,
        SelectQueryInfo &) const override
    {
        return to_stage;
    }

private:
    /// Common non-negative representation used to compare supported temporal types.
    /// Nanoseconds preserve DateTime64 ordering within a second; `system.query_log.abr_min_date` records whole seconds.
    struct UnixTimePoint
    {
        UInt64 seconds;
        UInt32 nanoseconds;

        bool operator<(const UnixTimePoint & rhs) const
        {
            return seconds < rhs.seconds || (seconds == rhs.seconds && nanoseconds < rhs.nanoseconds);
        }

        bool operator>=(const UnixTimePoint & rhs) const { return !(*this < rhs); }
    };

    /// Extracts a combined filter DAG from WHERE + PREWHERE clauses.
    std::optional<ActionsDAG> buildFilterActionsDAG(
        const SelectQueryInfo & query_info,
        ContextPtr local_context) const;

    /// Returns the earliest time value across parts that survive retention pruning.
    /// Parts without a minmax index are skipped; pruned parts are excluded.
    std::optional<UnixTimePoint> getMinMergeTreeDate(
        const MergeTreeData::SnapshotData & snapshot,
        const KeyCondition & retention_condition) const;

    /// Extracts the tightest finite lower bound on the time column from a KeyCondition.
    /// Returns nullopt when no finite lower bound can be determined.
    std::optional<UnixTimePoint> getQueryLowerBound(const KeyCondition & time_cond) const;

    /// Converts the time column's native numeric representation to Unix time.
    /// Returns nullopt when the value is before the Unix epoch.
    std::optional<UnixTimePoint> timeValueToUnixTimePoint(const Field & value) const;

    /// Picks the finest-grained table whose data covers the query's time range.
    /// Iterates finest→coarsest, prunes parts via the retention condition, and
    /// compares each table's surviving min date against the query lower bound.
    /// Falls back via pickFallbackTable() if none match.
    /// Records the skipped tables and the query lower bound in the `ABRQueryInfo` of the context.
    size_t chooseBestTable(SelectQueryInfo & query_info, ContextPtr local_context) const;

    /// Index in `tables` of the table with the given sample interval. Throws if there is none.
    size_t getTableIndexFromSuffix(UInt64 suffix) const;

    /// Index in `tables` of the coarsest table a query may be executed on: the one of abr_last_table_suffix,
    /// or the coarsest table if it is not set. Bounds both the choice of the table and the retries.
    size_t getLastCandidateTableIndex(const ContextPtr & local_context) const;

    /// Resolves the fallback table when no underlying table covers the query lower bound
    /// Picks the smallest min_storage_date at day precision
    /// On ties prefers the finest sample interval (lowest index)
    size_t pickFallbackTable(
        const std::vector<std::optional<UnixTimePoint>> & min_storage_dates,
        size_t start) const;

    /// Copies columns, primary key, partition key and sorting key from an underlying table.
    void copyTableMetadata(const StoragePtr & storage);

    std::vector<UInt64> sample_intervals;
    std::vector<StoragePtr> tables;

    String time_column;

    /// Per-column metadata extracted from the partition key's minmax index.
    struct ColumnInfo
    {
        DataTypePtr type;
        size_t idx_in_minmax;
    };
    std::map<String, ColumnInfo> column_info_minmax;
    Names retention_expr_col_names;
    DataTypes retention_expr_col_types;

    /// Identity expression over retention columns, used as the key expression for KeyCondition.
    ExpressionActionsPtr retention_key_expr;

    AtomicLogger logger;
};
}
