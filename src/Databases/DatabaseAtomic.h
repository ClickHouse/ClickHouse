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
struct DatabaseSchemaWALExactRepairProvenance;
struct AtomicDatabaseUDTPersistedConfigurationV2;
class PreparedAtomicDatabaseUDTConfigurationV2;
class BoundObjectTypeReferences;
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
struct PersistedTypeReferences;
struct SchemaObjectID;
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
public

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
    void assertUDTNewDefinitionClosureOperationAllowed(
        const UDT::BoundObjectTypeReferences & bound_references, UDT::AuthorityQuarantineOperationKind kind) const;
    void assertUDTDatabaseAllowsDetach(std::string_view operation) const;
    [[nodiscard]] UDTDetachGuard acquireUDTDatabaseDetachGuard(std::string_view operation) const;

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

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(ContextPtr /*context*/) override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

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

    bool isReservedMetadataDirectory(const String & directory_name) const override;:
    void commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path, const String & statement, ContextPtr query_context) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path, ContextPtr query_context) override;

    void assertDetachedTableNotInUse(const UUID & uuid) TSA_REQUIRES(mutex);
    using DetachedTables = std::unordered_map<UUID, StoragePtr>;
    [[nodiscard]] DetachedTables cleanupDetachedTables() TSA_REQUIRES(mutex);

    void createDirectories();
    void createDirectoriesUnlocked() TSA_REQUIRES(mutex);

    void tryCreateMetadataSymlink();
    void reclaimRetiredUDTRootsNoThrow() noexcept;

    UDT::AtomicAuthority &
    initializeUDTAuthorityUnlocked(std::unique_ptr<const UDT::AuthorityRoot> recovered_root, bool activate_recovered_authority)
        TSA_REQUIRES(udt_authority_mutex);
    [[nodiscard]] UDT::PreparedAtomicDatabaseUDTConfigurationV2 prepareConfiguredUDTConfigurationForFirstActivationV2();
    [[nodiscard]] const UDT::EffectiveResourceLimits & getConfiguredUDTEffectiveDatabaseLimitsForFirstActivation() const;
    void applyConfiguredUDTVerificationLimitsForFirstActivation(UDT::AuthorityRootBuildLimits & limits) const;
    void transitionPendingUDTAuthorityToDegraded(std::unique_lock<std::mutex> schema_mutation_lock);

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
    std::atomic<UDT::AtomicAuthority *> active_udt_authority{nullptr};
    std::atomic<UDT::AuthorityVerificationRuntimeState *> active_udt_verification_runtime{nullptr};
    std::atomic<bool> udt_database_startup_complete{false};
    bool udt_authority_shutdown TSA_GUARDED_BY(udt_authority_mutex) = false;
    friend class UDT::AuthorityVerificationBatchExecutor;
    friend class UDT::AuthorityVerificationBatchExecutorAccess;
    friend class UDT::AuthorityVerificationScheduler;
    friend class UDT::AuthorityRepairCoordinator;
    friend class UDT::AuthorityAutomaticRepair;
    friend class UDT::AuthorityAutomaticRepairAccess;

    LoadTaskPtr startup_atomic_database_task TSA_GUARDED_BY(mutex);

private

    [[nodiscard]] std::shared_ptr<const UDT::AuthorityVerificationBatchReceipt> executeUDTAuthorityVerificationBatch(
        const UDT::AuthorityVerificationBatchPlan & plan,
        const UDT::AuthorityVerificationBatchExecutorLimits & limits,
        bool wait_for_startup = true,
        const UDT::AuthorityVerificationBatchReceipt * verified_prefix = nullptr);

    friend class DatabaseOnDisk;
};

} // namespace DB
