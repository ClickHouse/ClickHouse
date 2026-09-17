#pragma once

#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabasesCommon.h>
#include <Storages/IStorage_fwd.h>

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

namespace UDT
{
class AtomicAuthority;
class AtomicAuthorityStartupStatusSnapshot;
class AtomicDatabaseSchemaMutationStorage;
struct DatabaseSchemaWALExactRepairProvenance;
struct AtomicDatabaseUDTPersistedConfigurationV2;
class PreparedAtomicDatabaseUDTConfigurationV2;
class AtomicLifecycleAdapter;
class BoundObjectTypeReferences;
struct AtomicTableStartupState;
struct CompletedTableColumnTypeAlterPublication;
class AuthorityRoot;
class AuthorityVerificationBatchExecutor;
class AuthorityVerificationBatchExecutorAccess;
class AuthorityVerificationBatchPlan;
class AuthorityVerificationBatchReceipt;
struct AuthorityVerificationBatchExecutorLimits;
struct AuthorityVerificationScheduleCursor;
struct AuthorityRootBuildLimits;
class AuthorityVerificationRuntimeState;
class AuthorityVerificationScheduler;
class AuthorityStorageReadContinuationEvidence;
class AuthorityStorageNewOperationCommitGuard;
class AuthorityAutomaticRepair;
class AuthorityAutomaticRepairAccess;
struct AuthorityVerificationSchedulerStatus;
struct AuthorityVerificationSchedulerLimits;
class EffectiveResourceLimits;
class AuthorityRepairCoordinator;
struct AuthorityQuarantineAdmissionDecision;
struct AuthorityQuarantineAdmissionLimits;
struct AuthorityQuarantineOperationView;
enum class AuthorityQuarantineOperationKind : UInt8;
class IAuthorityAdapter;
class ILifecycleAdapter;
struct PreparedTableColumnTypeBindings;
class PreparedStoredObjectTypeBindingHandoff;
struct PersistedTypeReferences;
struct SchemaObjectID;
enum class StoredObjectSourceMode : UInt8;
struct TypeAuthorityCapabilities;
} // namespace UDT

/// All tables in DatabaseAtomic have persistent UUID and store data in
/// /clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
/// where xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy is UUID of the table.
/// RENAMEs are performed without changing UUID and moving table data.
/// Tables in Atomic databases can be accessed by UUID through DatabaseCatalog.
/// On DROP TABLE no data is removed, DatabaseAtomic just marks table as dropped
/// by moving metadata to /clickhouse_path/metadata_dropped/ and notifies
/// DatabaseCatalog. Running queries still may use dropped table. Table will be
/// actually removed when it's not in use. Allows to execute RENAME and DROP
/// without IStorage-level RWLocks
class DatabaseAtomic : public DatabaseOrdinary
{
public:
    /// Retains the Atomic schema-mutation lock and one exact dependent-object-capable root for
    /// table-column binding through durable CREATE publication. The opaque
    /// implementation keeps authority/storage internals out of this header.
    class TableCreateGuard final
    {
    public:
        TableCreateGuard(const TableCreateGuard &) = delete;
        TableCreateGuard & operator=(const TableCreateGuard &) = delete;
        TableCreateGuard(TableCreateGuard &&) noexcept;
        TableCreateGuard & operator=(TableCreateGuard &&) noexcept;
        ~TableCreateGuard();

        const UDT::IAuthorityAdapter & getAuthorityAdapter() const noexcept;

    private:
        class Impl;
        explicit TableCreateGuard(std::unique_ptr<Impl> impl_) noexcept;

        friend class DatabaseAtomic;
        std::unique_ptr<Impl> impl;
    };

    class CrossDatabaseMoveGuard final
    {
    public:
        CrossDatabaseMoveGuard(const CrossDatabaseMoveGuard &) = delete;
        CrossDatabaseMoveGuard & operator=(const CrossDatabaseMoveGuard &) = delete;
        CrossDatabaseMoveGuard(CrossDatabaseMoveGuard &&) noexcept = default;
        CrossDatabaseMoveGuard & operator=(CrossDatabaseMoveGuard &&) noexcept = default;

    private:
        enum class Kind : UInt8
        {
            Source,
            Target,
        };

        CrossDatabaseMoveGuard(
            std::unique_lock<std::mutex> lock_,
            Kind kind_,
            StoragePtr source_table_,
            String source_table_name_,
            UUID source_table_uuid_,
            String source_relative_table_path_,
            String target_table_name_for_logs_,
            UUID target_incoming_table_uuid_) noexcept;

        friend class DatabaseAtomic;

        std::unique_lock<std::mutex> lock;
        Kind kind;
        StoragePtr source_table;
        String source_table_name;
        UUID source_table_uuid;
        String source_relative_table_path;
        String target_table_name_for_logs;
        UUID target_incoming_table_uuid;
    };

    /// Serializes an ordinary DETACH boundary with mapped-table ALTER/admission.
    /// The interpreter acquires every affected table's ALTER lock first, then
    /// retains this database-schema lock through per-table shutdown and
    /// dependency removal. After taking the exclusive database DDL guard, the
    /// interpreter releases it before catalog detachment because
    /// DatabaseAtomic::shutdown() enters the same schema mutex. This preserves
    /// the global table -> schema lock order used by ALTER without self-deadlock.
    class UDTDetachGuard final
    {
    public:
        UDTDetachGuard(const UDTDetachGuard &) = delete;
        UDTDetachGuard & operator=(const UDTDetachGuard &) = delete;
        UDTDetachGuard(UDTDetachGuard &&) noexcept = default;
        UDTDetachGuard & operator=(UDTDetachGuard &&) noexcept = default;

    private:
        enum class Kind : UInt8
        {
            Table,
            Database,
        };

        UDTDetachGuard(std::unique_lock<std::mutex> lock_, Kind kind_, StoragePtr expected_table_) noexcept;

        friend class DatabaseAtomic;
        std::unique_lock<std::mutex> lock;
        Kind kind;
        StoragePtr expected_table;
    };

    DatabaseAtomic(
        String name_,
        String metadata_path_,
        UUID uuid,
        const String & logger_name,
        ContextPtr context_,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});
    DatabaseAtomic(
        String name_,
        String metadata_path_,
        UUID uuid,
        ContextPtr context_,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});
    ~DatabaseAtomic() override;

    String getEngineName() const override { return "Atomic"; }
    UUID getUUID() const override { return db_uuid; }

    const UDT::TypeAuthorityCapabilities & getSupportedUDTAuthorityCapabilities() const noexcept override;
    const UDT::IAuthorityAdapter & getUDTAuthorityAdapter() const noexcept override;
    UDT::ILifecycleAdapter & getUDTLifecycleAdapter() noexcept override;

    /// No-throw half of first activation, called only after the epoch-1
    /// definition-only publication and its durable commit. An empty holder remains
    /// private until this invariant-checked release-store.
    void activateUDTAuthorityAfterFirstPublication() noexcept;
    bool hasActiveUDTAuthority() const noexcept;
    /// A tombstone reconstructed from metadata_dropped has no logical
    /// provenance and cannot recreate a removed sidecar/edge image. This
    /// remains ambiguous after the last definition is dropped, so UNDROP must
    /// test the durable marker rather than only the active root contents.
    bool hasDurableUDTAuthorityState() const;

    /// Lock-free quarantine gate over one immutable database-owned runtime
    /// snapshot. Callers must supply the complete exact touch/proof view owned
    /// by their operation boundary; an inactive/shut-down runtime fails closed.
    [[nodiscard]] UDT::AuthorityQuarantineAdmissionDecision decideUDTQuarantineAdmission(
        const UDT::AuthorityQuarantineOperationView & operation, const UDT::AuthorityQuarantineAdmissionLimits & limits) const noexcept;
    [[nodiscard]] UDT::AuthorityVerificationScheduleCursor getUDTAuthorityVerificationCursor() const;
    [[nodiscard]] UDT::AuthorityVerificationSchedulerStatus getUDTAuthorityVerificationSchedulerStatus() const noexcept;
    /// Replaces the resolved process-global scheduler layer before authority
    /// initialization. DatabaseAtomic still reconciles and decodes its own
    /// UUID-bound durable database policy layer, then derives the effective policy.
    void configureUDTAuthorityVerificationSchedulerForStartup(const UDT::AuthorityVerificationSchedulerLimits & effective_limits);
    [[nodiscard]] std::shared_ptr<const UDT::AuthorityStorageReadContinuationEvidence>
    acquireUDTStorageReadContinuationEvidence(const StorageMetadataPtr & metadata) const;
    void assertUDTStorageReadContinuationAllowed(
        const UDT::AuthorityStorageReadContinuationEvidence & evidence, const StorageMetadataPtr & metadata) const;
    void assertUDTNewStorageOperationAllowed(const StorageMetadataPtr & metadata, UDT::AuthorityQuarantineOperationKind kind) const;
    [[nodiscard]] UDT::AuthorityStorageNewOperationCommitGuard
    acquireUDTNewStorageOperationCommitGuard(const StorageMetadataPtr & metadata, UDT::AuthorityQuarantineOperationKind kind) const;
    /// Stable database-owned check used by query-cache hits before Analyzer or
    /// storage operation gates run. It observes the immutable authority root
    /// under the same publication mutex, so a durable physical-to-mapped
    /// transition cannot be hidden by the storage's still-old metadata.
    [[nodiscard]] bool hasDatabaseOwnedUDTObject(UUID object_uuid) const;
    [[nodiscard]] bool hasDatabaseOwnedUDTObjectForQueryCache(UUID object_uuid) const;
    void assertUDTNewDefinitionClosureOperationAllowed(
        const UDT::BoundObjectTypeReferences & bound_references, UDT::AuthorityQuarantineOperationKind kind) const;

    /// Preflight for interpreters whose ordinary DETACH/ATTACH paths perform
    /// irreversible or externally visible work before the database callback.
    /// These throw before that work when the table UUID belongs to the durable
    /// mapped-table inventory.
    void assertUDTTableAllowsOrdinaryMetadataMutation(const StoragePtr & table, ContextPtr context, std::string_view operation) const;
    void assertUDTTableUUIDAllowsOrdinaryMetadataMutation(UUID table_uuid, std::string_view table_name, std::string_view operation) const;
    /// Rejects an independent mutation of the generated physical inner table
    /// while a durable mapped MaterializedView still owns it. Interpreters use
    /// this before shutdown, dependency removal, or distributed dispatch; the
    /// database callback repeats the check under its final mutation lock.
    void assertUDTPhysicalInnerTableOperationAllowed(const StoragePtr & table, std::string_view operation) const;
    /// Name-only variant for ATTACH/dispatch boundaries where the generated
    /// child is not live yet but the mapped owner identity is durable.
    void assertUDTPhysicalInnerTableNameOperationAllowed(std::string_view table_name, std::string_view operation) const;
    void assertUDTDatabaseAllowsDetach(std::string_view operation) const;

    /// The caller must hold the affected table ALTER lock (all table ALTER
    /// locks, in deterministic order, for the database form) before entering
    /// these boundaries and retain it for at least as long as the guard.
    [[nodiscard]] UDTDetachGuard acquireUDTTableDetachGuard(const StoragePtr & table, ContextPtr context, std::string_view operation) const;
    [[nodiscard]] UDTDetachGuard acquireUDTDatabaseDetachGuard(std::string_view operation) const;
    StoragePtr
    detachTableUnderUDTGuard(ContextPtr context, const String & name, const StoragePtr & expected_table, const UDTDetachGuard & guard);
    void detachTablePermanentlyUnderUDTGuard(
        ContextPtr context, const String & name, const StoragePtr & expected_table, const UDTDetachGuard & guard);

    /// Explicit dependent-object admission boundary. Definition DDL deliberately leaves
    /// a fresh authority at the permanent definition-only root. A dependent-object
    /// mutation must call this while already protected by its normal owning
    /// database/table DDL guard, and before it pins, binds, validates, or plans
    /// against an authority root: activation advances database_catalog_epoch.
    /// The transition is serialized, durable, idempotent, and is never part of
    /// CREATE TYPE's success contract.
    void ensureUDTDependentObjectCapabilities();

    /// Acquires the exact dependent-object-capable authority/root/storage tuple used by the
    /// table binder and later consumed by
    /// createTableWithUDTBindings. The caller must already hold the
    /// ordinary table-name DDL guard.
    [[nodiscard]] TableCreateGuard acquireUDTTableCreateGuard();

    /// Requires an exact same-authority logical source-sidecar admission while
    /// the retained CREATE guard still pins the publication root.
    void authorizeUDTTableSourceSidecarCopy(
        const TableCreateGuard & guard,
        UDT::StoredObjectSourceMode source_mode,
        const UDT::PersistedTypeReferences & source_references,
        const UDT::BoundObjectTypeReferences & bound_source_references) const;

    /// Production mapped-table CREATE boundary for fresh local Atomic Memory
    /// and non-replicated, non-shared MergeTree-family tables. It starts and
    /// binds the physical storage before publication, durably installs metadata,
    /// sidecar, expectation, installation mapping and graph as one mutation,
    /// then publishes the authority, UUID mapping and table catalog while the
    /// database mutex still hides the live table.
    void createTableWithUDTBindings(
        TableCreateGuard guard,
        ContextPtr query_context,
        const ASTPtr & physical_create_query,
        const StoragePtr & table,
        UDT::PreparedTableColumnTypeBindings table_bindings,
        UDT::StoredObjectSourceMode selected_output_source_mode);

    /// Database-owned publication boundary for an exact physicalized
    /// View/MaterializedView/Dictionary binding package.
    void createStoredObjectWithUDTBindings(
        TableCreateGuard guard,
        ContextPtr query_context,
        const ASTPtr & physical_create_query,
        const StoragePtr & object_storage,
        UDT::PreparedStoredObjectTypeBindingHandoff bindings);

    bool empty() const override;
    bool emptyForDrop() const override;
    void shutdown() override;

    void renameDatabase(ContextPtr query_context, const String & new_name) override;

    void renameTable(
        ContextPtr context,
        const String & table_name,
        IDatabase & to_database,
        const String & to_table_name,
        bool exchange,
        bool dictionary) override;

    void dropTable(ContextPtr context, const String & table_name, bool sync) override;
    void dropTableImpl(ContextPtr context, const String & table_name, bool sync);

    void attachTable(ContextPtr context, const String & name, const StoragePtr & table, const String & relative_table_path) override;
    StoragePtr detachTable(ContextPtr context, const String & name) override;
    void alterTable(
        ContextPtr context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, bool validate_new_create_query) override;
    bool rollbackUDTTableAlter(
        ContextPtr context,
        const StorageID & table_id,
        StorageInMemoryMetadata & metadata_to_restore,
        const StorageInMemoryMetadata & committed_metadata) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(ContextPtr /*context*/) override;

    DatabaseTablesIteratorPtr
    getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    std::vector<std::pair<ASTPtr, StoragePtr>>
    getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & context) const override;
    /// Type-erased RAII lease used by BackupEntriesCollector after it has
    /// acquired the selected tables' share locks. The lease rechecks that no
    /// selected table is mapped and then blocks Atomic ALTER/admission until
    /// metadata and data backup entries have been emitted.
    [[nodiscard]] std::shared_ptr<void> acquireUDTBackupLease(const std::vector<StoragePtr> & selected_tables, ContextPtr context) const;
    void createTableRestoredFromBackup(
        const ASTPtr & create_table_query,
        ContextMutablePtr context,
        std::shared_ptr<IRestoreCoordination> restore_coordination,
        UInt64 timeout_ms) override;

    void beforeLoadingMetadata(ContextMutablePtr context, LoadingStrictnessLevel mode) override;

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;
    void waitDatabaseStarted() const override;
    void stopLoading() override;

    /// Atomic database cannot be detached if there is detached table which still
    /// in use
    void assertCanBeDetached(bool cleanup) override;

    UUID tryGetTableUUID(const String & table_name) const override;

    void tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist = false);
    void tryRemoveSymlink(const String & table_name);

    void waitDetachedTableNotInUse(const UUID & uuid, std::function<void()> throw_if_cancelled) override;
    void checkDetachedTableNotInUse(const UUID & uuid) override;
    void setDetachedTableNotInUseForce(const UUID & uuid) override;

protected:
    enum class AuthorityMode : UInt8
    {
        Enabled,
        Unsupported,
    };

    DatabaseAtomic(
        String name_,
        String metadata_path_,
        UUID uuid,
        const String & logger_name,
        ContextPtr context_,
        AuthorityMode udt_authority_mode_,
        DatabaseMetadataDiskSettings database_metadata_disk_settings_ = {});

    /// Holds dependent-object admission serialization for the generic cross-database
    /// move path. Only DatabaseOnDisk may consume the resulting capability;
    /// derived test databases may acquire one to verify lock lifetime.
    [[nodiscard]] CrossDatabaseMoveGuard
    acquireUDTCrossDatabaseTargetGuard(UUID incoming_table_uuid, std::string_view table_name_for_logs) TSA_NO_THREAD_SAFETY_ANALYSIS;
    [[nodiscard]] CrossDatabaseMoveGuard acquireUDTCrossDatabaseSourceGuard(
        const String & name, const StoragePtr & table, const String & relative_table_path, ContextPtr local_context)
        TSA_NO_THREAD_SAFETY_ANALYSIS;

    bool isReservedMetadataDirectory(const String & directory_name) const override;

    StoredObjectMetadataLoadDecision
    decideStoredObjectMetadataLoadBeforeParsing(std::string_view canonical_file_object_name) const override;
    StoredObjectMetadataLoadDecision
    decideStoredObjectMetadataLoadAfterParsing(std::string_view canonical_file_object_name, const ASTCreateQuery & query) const override;
    bool forceEagerTableLoadAtStartup(const ASTCreateQuery & query) const override;
    void validateTableMetadataForLoading(const ASTCreateQuery & query, bool permanently_detached) const override;
    void validateTableMetadataRewriteBeforeLoading(const ASTCreateQuery & query) const override;
    void
    onAsyncTableLoadingFailed(const QualifiedTableName & name, AsyncTableLoadingFailurePhase phase, std::exception_ptr failure) override;
    void onTableStartupCompleted(const QualifiedTableName & name, const StoragePtr & table) override;

    void commitAlterTable(
        const StorageID & table_id,
        const String & table_metadata_tmp_path,
        const String & table_metadata_path,
        const String & statement,
        ContextPtr query_context) override;
    void commitCreateTable(
        const ASTCreateQuery & query,
        const StoragePtr & table,
        const String & table_metadata_tmp_path,
        const String & table_metadata_path,
        ContextPtr query_context) override;

    void assertDetachedTableNotInUse(const UUID & uuid) TSA_REQUIRES(mutex);
    using DetachedTables = std::unordered_map<UUID, StoragePtr>;
    [[nodiscard]] DetachedTables cleanupDetachedTables() TSA_REQUIRES(mutex);
    void attachTableWithoutUDTGuard(const String & name, const StoragePtr & table, const String & relative_table_path);
    void cleanupDetachedTablesBeforeAttachWithoutSchemaGuard();
    [[nodiscard]] std::pair<StoragePtr, DetachedTables>
    detachTableWithoutUDTGuard(const String & name, const StoragePtr & expected_table, bool cleanup_detached_tables);

    void createDirectories();
    void createDirectoriesUnlocked() TSA_REQUIRES(mutex);

    void tryCreateMetadataSymlink();

    bool hasDatabaseOwnedTableExpectationForCrossDatabaseMove(UUID table_uuid) const;
    bool hasDatabaseOwnedUDTTableBinding(const StoragePtr & table, ContextPtr local_context) const;
    void assertNotLiveMappedMaterializedViewInnerTable(const StoragePtr & table, std::string_view operation) const
        TSA_REQUIRES(udt_schema_mutation_mutex);
    void assertNotLiveMappedMaterializedViewInnerTable(std::string_view table_name, std::string_view operation) const
        TSA_REQUIRES(udt_schema_mutation_mutex);
    void reclaimRetiredUDTRootsNoThrow() noexcept;
    UDT::CompletedTableColumnTypeAlterPublication alterUDTStoredObject(
        ContextPtr context,
        const StorageID & table_id,
        const StoragePtr & table,
        const StorageInMemoryMetadata & metadata,
        bool validate_new_create_query,
        bool trusted_boundary_rollback = false,
        const StorageInMemoryMetadata * expected_current_metadata = nullptr) TSA_REQUIRES(udt_schema_mutation_mutex);
    void dropUDTTable(ContextPtr context, const String & table_name, const StoragePtr & table, bool sync)
        TSA_REQUIRES(udt_schema_mutation_mutex);
    void dropTableImplWithoutUDTGuard(ContextPtr context, const String & table_name, bool sync);

    UDT::AtomicAuthority &
    initializeUDTAuthorityUnlocked(std::unique_ptr<const UDT::AuthorityRoot> recovered_root, bool activate_recovered_authority)
        TSA_REQUIRES(udt_authority_mutex);
    [[nodiscard]] UDT::PreparedAtomicDatabaseUDTConfigurationV2 prepareConfiguredUDTConfigurationForFirstActivationV2();
    [[nodiscard]] const UDT::EffectiveResourceLimits & getConfiguredUDTEffectiveDatabaseLimitsForFirstActivation() const;
    void applyConfiguredUDTVerificationLimitsForFirstActivation(UDT::AuthorityRootBuildLimits & limits) const;
    void assertUDTTypeLifecycleOperationAllowed(
        const UDT::AuthorityRoot * exact_active_root,
        std::span<const UDT::SchemaObjectID> sorted_unique_touched_objects,
        std::string_view operation) const TSA_REQUIRES(udt_schema_mutation_mutex);
    void markUDTTableStartupSucceeded(UUID table_uuid, std::string_view table_name, const StoragePtr & table)
        TSA_REQUIRES(udt_schema_mutation_mutex);
    void activateUDTAuthorityAfterPendingTableStartup() TSA_REQUIRES(udt_schema_mutation_mutex);
    void transitionPendingUDTAuthorityToDegraded(std::unique_lock<std::mutex> schema_mutation_lock);
    void handlePendingUDTTableLoadOrStartupFailure(
        const QualifiedTableName & name, AsyncTableLoadingFailurePhase phase, std::exception_ptr failure);

    virtual bool allowMoveTableToOtherDatabaseEngine(IDatabase & /*to_database*/) const { return false; }

    // TODO store path in DatabaseWithOwnTables::tables
    using NameToPathMap = std::unordered_map<String, String>;
    NameToPathMap table_name_to_path TSA_GUARDED_BY(mutex);

    DetachedTables detached_tables TSA_GUARDED_BY(mutex);
    std::filesystem::path path_to_table_symlinks;
    std::filesystem::path path_to_metadata_symlink;
    const UUID db_uuid;

    const AuthorityMode udt_authority_mode;
    struct UDTAuthorityConfiguration;
    std::unique_ptr<UDT::AtomicLifecycleAdapter> udt_lifecycle_adapter;
    mutable std::mutex udt_schema_mutation_mutex;
    mutable std::mutex udt_authority_mutex;
    /// A successful RESTORE preflight retains one bounded lease through the
    /// restored object's metadata/catalog publication. The counter is atomic
    /// because lease release happens outside schema serialization; admission
    /// and final-publication observations still occur while holding the schema
    /// mutex.
    std::atomic<UInt64> udt_restore_publication_leases{0};
    std::unique_ptr<UDT::AtomicAuthority> udt_authority;
    std::unique_ptr<UDT::AtomicDatabaseSchemaMutationStorage> udt_mutation_storage;
    std::shared_ptr<const UDT::AtomicAuthorityStartupStatusSnapshot> udt_degraded_startup_status TSA_GUARDED_BY(udt_authority_mutex);
    std::unique_ptr<UDT::AuthorityVerificationRuntimeState> udt_verification_runtime;
    std::unique_ptr<UDT::AuthorityVerificationScheduler> udt_verification_scheduler;
    std::unique_ptr<const UDT::DatabaseSchemaWALExactRepairProvenance> udt_last_exact_repair_provenance TSA_GUARDED_BY(udt_authority_mutex);
    std::unique_ptr<UDTAuthorityConfiguration> udt_authority_configuration;
    std::unique_ptr<UDT::AtomicTableStartupState> udt_table_startup_state TSA_GUARDED_BY(udt_authority_mutex);
    std::atomic<UDT::AtomicAuthority *> active_udt_authority{nullptr};
    std::atomic<UDT::AuthorityVerificationRuntimeState *> active_udt_verification_runtime{nullptr};
    std::atomic<bool> udt_database_startup_complete{false};
    bool udt_authority_shutdown TSA_GUARDED_BY(udt_authority_mutex) = false;

    friend class UDT::AtomicLifecycleAdapter;
    friend class UDT::AuthorityVerificationBatchExecutor;
    friend class UDT::AuthorityVerificationBatchExecutorAccess;
    friend class UDT::AuthorityVerificationScheduler;
    friend class UDT::AuthorityRepairCoordinator;
    friend class UDT::AuthorityAutomaticRepair;
    friend class UDT::AuthorityAutomaticRepairAccess;

    LoadTaskPtr startup_atomic_database_task TSA_GUARDED_BY(mutex);

private:
    struct MappedObjectAuthorityImage;

    /// Loads and cross-checks the exact durable image for one currently mapped
    /// object while the caller owns the Atomic schema-mutation boundary. The
    /// returned root snapshot pins the authority revision used by the image.
    [[nodiscard]] MappedObjectAuthorityImage loadExactMappedObjectAuthorityImage(UUID object_uuid, std::string_view object_name) const
        TSA_REQUIRES(udt_schema_mutation_mutex);
    [[nodiscard]] bool isExactTemporarilyDetachedUDTObject(const UDT::SchemaObjectID & object, std::string_view object_name) const
        TSA_REQUIRES(udt_schema_mutation_mutex);
    void validateMappedObjectForTemporaryDetach(const StoragePtr & table, ContextPtr context) const TSA_REQUIRES(udt_schema_mutation_mutex);
    [[nodiscard]] std::shared_ptr<const UDT::AuthorityStorageReadContinuationEvidence>
    acquireUDTStorageReadContinuationEvidenceImpl(const StorageMetadataPtr & metadata, UDT::AuthorityQuarantineOperationKind kind) const;

    [[nodiscard]] std::shared_ptr<const UDT::AuthorityVerificationBatchReceipt> executeUDTAuthorityVerificationBatch(
        const UDT::AuthorityVerificationBatchPlan & plan,
        const UDT::AuthorityVerificationBatchExecutorLimits & limits,
        bool wait_for_startup = true,
        const UDT::AuthorityVerificationBatchReceipt * verified_prefix = nullptr);
    void assertOwnsUDTDetachGuard(const UDTDetachGuard & guard, const StoragePtr & expected_table) const;
    void assertOwnsUDTCrossDatabaseGuard(const CrossDatabaseMoveGuard & guard, CrossDatabaseMoveGuard::Kind expected_kind) const;
    void releaseUDTCrossDatabaseGuardForNestedTableRename(CrossDatabaseMoveGuard & guard) const TSA_NO_THREAD_SAFETY_ANALYSIS;
    void reacquireUDTCrossDatabaseGuardAfterNestedTableRename(CrossDatabaseMoveGuard & guard) const TSA_NO_THREAD_SAFETY_ANALYSIS;
    void validateUDTCrossDatabaseGuardAfterNestedTableRename(
        const CrossDatabaseMoveGuard & guard, ContextPtr local_context) const TSA_NO_THREAD_SAFETY_ANALYSIS;
    void attachTableUnderUDTCrossDatabaseGuard(
        const String & name, const StoragePtr & table, const String & relative_table_path, const CrossDatabaseMoveGuard & guard)
        TSA_NO_THREAD_SAFETY_ANALYSIS;
    StoragePtr
    detachTableUnderUDTCrossDatabaseGuard(const String & name, const CrossDatabaseMoveGuard & guard) TSA_NO_THREAD_SAFETY_ANALYSIS;

    friend class DatabaseOnDisk;
};

} // namespace DB
