#pragma once

#include <Parsers/ASTViewTargets.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <array>


namespace DB
{
struct TimeSeriesSettings;
using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

/// Represents a table engine to keep time series received by Prometheus protocols.
/// Examples of using this table engine:
///
/// CREATE TABLE ts ENGINE = TimeSeries()
/// -OR-
/// CREATE TABLE ts ENGINE = TimeSeries() SAMPLES [db].table1 TAGS [db].table2 METRIC FAMILIES [db].table3
/// -OR-
/// CREATE TABLE ts ENGINE = TimeSeries() SAMPLES ENGINE = MergeTree TAGS ENGINE = ReplacingMergeTree METRIC FAMILIES ENGINE = ReplacingMergeTree
/// -OR-
/// CREATE TABLE ts ENGINE = TimeSeries()
///    SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
///    SAMPLES ENGINE = ReplicatedMergeTree('zkpath', 'replica')
///    TAGS INNER COLUMNS (
///        id UUID DEFAULT reinterpretAsUUID(sipHash128(tags)) CODEC(ZSTD(3)),
///        instance LowCardinality(String),
///        job String)
///    ENGINE = ReplacingMergeTree, ...
///
class StorageTimeSeries final : public StorageWithCommonVirtualColumns, WithContext
{
public:
    StorageTimeSeries(const StorageID & table_id, const ContextPtr & local_context,
                      LoadingStrictnessLevel mode, bool is_restore_from_backup,
                      const ASTCreateQuery & query, const ColumnsDescription & columns, const String & comment);

    ~StorageTimeSeries() override;

    std::string getName() const override { return "TimeSeries"; }

    std::shared_ptr<const TimeSeriesSettings> getStorageSettings() const { return storage_settings.get(); }

    /// Returns the schema version of this table (the `version` setting, see TimeSeriesVersion.h).
    UInt64 getVersion() const;

    /// Returns the target table (works for both inner and external targets).
    StoragePtr getTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const;
    StoragePtr tryGetTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const;
    StorageID getTargetTableID(ViewTarget::Kind target_kind, const ContextPtr & local_context) const;
    StorageID tryGetTargetTableID(ViewTarget::Kind target_kind, const ContextPtr & local_context) const;

    struct LockedTargetTable
    {
        StoragePtr table;
        StorageMetadataPtr metadata;
        TableLockHolder share_lock;
    };

    /// Pin the target table and record its metadata epoch before binding a plan.
    LockedTargetTable lockAndValidateBucketedSamplesTarget(
        ViewTarget::Kind target_kind,
        const StoragePtr & expected_table,
        const StorageID & expected_id,
        const ContextPtr & local_context) const;

    /// Reject an ALTER or target replacement which raced with binding the nested plan.
    void revalidateBucketedSamplesTarget(
        ViewTarget::Kind target_kind,
        const LockedTargetTable & locked_target,
        const StorageID & expected_id,
        const ContextPtr & local_context) const;

    /// RESTORE calls this after all tables are created but before any data is inserted.
    /// An absent external target may be restored separately, but an inner target must exist.
    void validateBucketedSamplesTargets(const ContextPtr & local_context, bool allow_missing_external_targets = false) const;

    bool isInnerTable(ViewTarget::Kind target_kind) const;
    bool hasInnerTables() const { return has_inner_tables; }

    /// Whether this table has a target of the given kind (the RecentSamples target is optional).
    bool hasTarget(ViewTarget::Kind target_kind) const;

    /// Returns all possible target kinds: Samples, RecentSamples, Tags, and MetricFamilies.
    /// A concrete table can have no RecentSamples target (see hasTarget).
    static constexpr std::array<ViewTarget::Kind, 4> getTargetKinds()
    {
        return {ViewTarget::Samples, ViewTarget::RecentSamples, ViewTarget::Tags, ViewTarget::MetricFamilies};
    }

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    /// With `FINAL` the read deduplicates unmerged rows of the inner "tags" table, so a series is returned
    /// exactly once (see `makeASTSelectFromTimeSeries`).
    bool supportsFinal() const override { return true; }

    static VirtualColumnsDescription createVirtuals();

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr local_context) override;

    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;

    /// Forward the size guard onto every inner target table that `dropInnerTableIfAny`
    /// will actually drop.
    void checkTableSizeBelowDropLimit(ContextPtr query_context) const override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;
    void alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder & table_lock_holder, DDLGuardPtr & ddl_guard) override;

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytesUncompressed(const Settings & settings) const override;
    Strings getDataPaths() const override;
#if CLICKHOUSE_CLOUD
    std::vector<StorageID> getInnerStorageIDs() const override;
#endif

private:
    /// Represents one of the target tables; `is_inner_table` is true when the table was auto-created by TimeSeries and is owned by it.
    struct Target
    {
        ViewTarget::Kind kind{};
        StorageID table_id = StorageID::createEmpty();
        bool is_inner_table = false;
    };

    /// Reads information about the target tables from the create query without creating anything.
    static std::vector<Target> findTargets(const ASTCreateQuery & create_query);

    /// Initializes information about the target tables and creates the inner ones (unless this is an ATTACH query).
    static std::vector<Target> buildTargets(
        const ASTCreateQuery & create_query,
        const StorageID & table_id,
        const ContextPtr & local_context, LoadingStrictnessLevel mode);

    /// Returns the target of the given kind or null if this table has no such target.
    const Target * tryGetTarget(ViewTarget::Kind target_kind) const;

    /// Implementation for getTargetTable() and tryGetTargetTable().
    StoragePtr getTargetTableImpl(ViewTarget::Kind target_kind, const ContextPtr & local_context, bool throw_if_not_found) const;

    void validateBucketedSamplesTarget(
        ViewTarget::Kind target_kind,
        const StoragePtr & target_table,
        const ContextPtr & local_context,
        bool bypass_metadata_cache = false) const;

    StorageMetadataPtr getValidatedBucketedSamplesTargetMetadata(
        ViewTarget::Kind target_kind,
        const StoragePtr & expected_table,
        const StorageID & expected_id,
        const ContextPtr & local_context) const;

    MultiVersion<TimeSeriesSettings> storage_settings;

    std::vector<Target> targets;
    bool has_inner_tables = false;
};

std::shared_ptr<StorageTimeSeries> storagePtrToTimeSeries(StoragePtr storage);
std::shared_ptr<const StorageTimeSeries> storagePtrToTimeSeries(ConstStoragePtr storage);

}
