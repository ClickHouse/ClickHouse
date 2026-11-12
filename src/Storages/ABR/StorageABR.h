#pragma once

#include <optional>
#include <span>

#include <Common/AtomicLogger.h>
#include <Core/Names.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageFactory.h>

namespace DB
{

/// Read-only proxy engine that routes queries to the best source MergeTree table.
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
    /// A query attempt against one of the source tables: an interpreter that is ready to
    /// execute, the header of the result it produces, and its context, with its share of the limits.
    struct Attempt
    {
        std::shared_ptr<IInterpreter> interpreter;
        SharedHeader header;
        ContextPtr context;
    };

    /// A source table resolved for a query, see `resolveSourceTable`.
    struct SourceTable
    {
        StoragePtr storage;
        UInt64 interval;
        /// Positions of the retention columns in the minmax index of the parts of the table.
        size_t time_idx_in_minmax;
        std::vector<size_t> retention_idx_in_minmax; /// In the order of `Retention::names`
    };

    /// Does not access the source tables, so that the table is always loadable. They are resolved on every use instead.
    StorageABR(
        const StorageID & table_id,
        const String & table_prefix_,
        const std::vector<UInt64> & sample_intervals_,
        const Names & retention_expression_);

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

    /// Formatted name for a source table: `{prefix}_{interval}`.
    static String getSourceTableName(const String & prefix, UInt64 interval);

    /// Names of the source tables referenced by the arguments of an `ABR` engine definition, in a given `database`.
    static TableNamesSet getSourceTableNames(const ASTFunction & engine, const String & database);

    String getName() const override { return "ABR"; }

    /// Because of the retry mechanism, ABR fully executes the query before returning
    /// getQueryProcessingStage() returns to_stage unchanged and ABR hands back mergeable state
    /// This flag lets the merge machinery treat that state correctly without marking it remote
    bool producesMergeableState(const ContextPtr & /*query_context*/) const override { return true; }

    /// The query is rewritten for the chosen source table, see `rewriteQueryForStorage`.
    bool readRequiresAnalyzedQuery() const override { return true; }

    /// The table is chosen from the filters of the query, and the whole query is retried on another table.
    bool readRequiresFullQuery() const override { return true; }

    /// `ABR` is read-only, so `TRUNCATE ALL TABLES` skips it, and truncates its source tables instead.
    bool supportsTruncate() const override { return false; }

    /// The query is executed on a source table, which is from the `MergeTree` family, which supports them.
    bool supportsColumnsWithDynamicStructure() const override { return true; }

    /// `ABR` has no columns of its own: they are those of its finest source table that exists, or none if no source table exists.
    /// A missing source table then fails the query in `read` with `UNKNOWN_TABLE` rather than with an unknown column.
    StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr query_context, bool bypass_metadata_cache) const override;

    /// The columns are those of the finest source table that exists, so showing them requires the access to that table.
    bool isExposedMetadataGranted(const ContextPtr & query_context, AccessType access_type, const String & column_name) const override;
    std::optional<NameSet> filterColumnsWithExposedMetadataGranted(
        const ContextPtr & query_context, AccessType access_type, const Names & column_names) const override;

    ColumnSizeByName getColumnSizes() const override;
    std::optional<ColumnSizeByName> tryGetColumnSizes(const ContextPtr & query_context) const override;
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
    ///   table_prefix:          String          — common prefix of source tables
    ///   sample_intervals:      Array(UInt64)   — granularity levels (sorted ascending)
    ///   retention_expression:  Tuple or String — partition columns used for part pruning;
    ///                                            first element is the time column
    static std::shared_ptr<StorageABR> creatorFunction(const StorageFactory::Arguments & args);

    /// The source tables are found in the database of the `ABR` table on every use, so it cannot be moved to another database.
    /// Renaming the database is allowed, as the source tables move with it.
    void checkTableCanBeRenamed(const StorageID & new_name) const override;
    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

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

    /// The retention columns with their types in the columns of the `ABR` table, resolved for a query.
    struct Retention
    {
        DataTypePtr time_type;
        /// Retention columns other than the time column.
        Names names;
        DataTypes types;
        /// Identity expression over all retention columns, used as the key expression for `KeyCondition`.
        ExpressionActionsPtr key_expr;
    };

    /// Throws `BAD_ARGUMENTS` if a retention column is missing from `columns`, or the time column is `Nullable`.
    Retention getRetention(const ColumnsDescription & columns, const ContextPtr & local_context) const;

    /// Checks every source table, see `resolveSourceTable`, and that they all have the same columns.
    void checkSourceTables(const ContextPtr & local_context) const;

    /// Extracts a combined filter DAG from WHERE + PREWHERE clauses.
    std::optional<ActionsDAG> buildFilterActionsDAG(
        const SelectQueryInfo & query_info,
        ContextPtr local_context) const;

    /// Returns the earliest time value across the parts of `table` that survive retention pruning.
    /// Parts without a minmax index are skipped; pruned parts are excluded.
    std::optional<UnixTimePoint> getMinMergeTreeDate(
        const SourceTable & table,
        const Retention & retention,
        const KeyCondition & retention_condition,
        const ContextPtr & local_context) const;

    /// Extracts the tightest finite lower bound on the time column from a KeyCondition.
    /// Returns nullopt when no finite lower bound can be determined.
    std::optional<UnixTimePoint> getQueryLowerBound(const KeyCondition & time_cond, const DataTypePtr & time_type) const;

    /// Finds the lower bound on the time column by a binary search over the values of the column.
    std::optional<UnixTimePoint> searchQueryLowerBound(const KeyCondition & time_cond, const DataTypePtr & time_type) const;

    /// Converts the time column's native numeric representation to Unix time.
    /// Returns nullopt when the value is before the Unix epoch.
    std::optional<UnixTimePoint> timeValueToUnixTimePoint(const Field & value, const DataTypePtr & time_type) const;

    /// Picks the finest-grained of `tables` whose data covers the query's time range, and returns its position in `tables`.
    /// Iterates finest→coarsest, prunes parts via the retention condition, and
    /// compares each table's surviving min date against the query lower bound.
    /// Falls back via pickFallbackTable() if none match.
    /// Records the skipped tables and the query lower bound in the `ABRQueryInfo` of the context.
    size_t chooseBestTable(
        SelectQueryInfo & query_info, ContextPtr local_context, const std::vector<SourceTable> & tables, const Retention & retention) const;

    /// Sample intervals of the tables a query may be executed on, from the one of `abr_first_table_suffix`
    /// to the one of `abr_last_table_suffix`, or all of them if the settings are not set.
    std::span<const UInt64> getCandidateIntervals(const ContextPtr & local_context) const;

    /// Finds the source table of `interval`. Throws `UNKNOWN_TABLE` if it does not exist.
    StoragePtr getSourceTable(UInt64 interval, const ContextPtr & local_context) const;

    /// Finds the source table of `interval` and checks that its partition key allows retention pruning.
    SourceTable resolveSourceTable(UInt64 interval, const Retention & retention, const ContextPtr & local_context) const;

    /// Resolves the source tables of `intervals`, see `resolveSourceTable`.
    std::vector<SourceTable> getSourceTables(
        std::span<const UInt64> intervals, const Retention & retention, const ContextPtr & local_context) const;

    /// Finds the finest source table that exists, with the global context, whose columns are the ones of this table.
    StoragePtr tryGetFinestSourceTable() const;

    /// Finds all the source tables with the global context, for methods that have no query context.
    /// Returns an empty vector if any of them does not exist.
    std::vector<StoragePtr> tryGetSourceTables() const;

    /// Resolves the fallback table when no source table covers the query lower bound
    /// Picks the smallest min_storage_date at day precision
    /// On ties prefers the finest sample interval (lowest index)
    size_t pickFallbackTable(const std::vector<std::optional<UnixTimePoint>> & min_storage_dates) const;

    String table_prefix;
    std::vector<UInt64> sample_intervals;
    /// Its first element is the time column.
    Names retention_expression;
    String time_column;
    AtomicLogger logger;
};
}
