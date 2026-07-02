#pragma once
#include <string>
#include <vector>
#include <Common/SettingsChanges.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace DiskFromAST
{
    /// Tracks custom-disk registrations performed during ALTER validation (or any other dry-run that
    /// calls `convertCustomDiskField`/`convertCustomDiskSettings`) so they can be rolled back when
    /// the validation later throws, while remaining safe under concurrent DDL.
    ///
    /// Ownership semantics. The global pending-rollback table keeps an "active owners" set per
    /// disk name plus a `committed` flag. The first scope to register a fresh custom disk inserts
    /// itself into the active set; any subsequent scope that observes the same name via
    /// `Context::getOrCreateDisk` joins the active set as an additional owner instead of erasing
    /// the entry. The disk is rolled back from the global `DiskSelector` only when (a) no scope
    /// has committed its metadata transition, AND (b) the LAST scope to leave the active set runs
    /// its destructor. A scope's `commit` flips the `committed` flag and drops its slot; from then
    /// on no surviving scope's destructor will roll back the disk, even if every observer later
    /// fails. This eliminates the TOCTOU race where a concurrent observer could erase the
    /// pending-rollback marker and then itself fail, leaving the original registrar unable to
    /// roll back a disk that nobody actually committed.
    ///
    /// Cache-entry ownership. `disk(type = cache, name = X, ...)` registers a `FileCache` keyed by
    /// `X` in the global `FileCacheFactory`; the rollback path removes that entry only when the
    /// FIRST registrar's snapshot proved the cache name did not pre-exist (recorded in
    /// `we_inserted_cache_entry` on the tentative-registration entry). A pre-existing cache reused
    /// via the same path is never silently torn down by an unrelated rejected ALTER.
    ///
    /// Usage at a validation call site (e.g. `MergeTreeData::checkAlterIsPossible`):
    ///
    ///     CustomDiskRegistrationScope disk_scope(local_context);
    ///     DiskFromAST::convertCustomDiskSettings(new_changes, local_context, false, &disk_scope);
    ///     // ... validation that may throw ...
    ///     // No `disk_scope.commit();` - destructor releases the slot; the disk is rolled back
    ///     // only when no other scope is in flight and none has committed.
    ///
    /// Usage at the actual apply site (e.g. `MergeTreeData::changeSettings`):
    ///
    ///     CustomDiskRegistrationScope disk_scope(getContext());
    ///     DiskFromAST::convertCustomDiskSettings(new_changes, getContext(), false, &disk_scope);
    ///     // ... apply work ...
    ///     disk_scope.commit();   /// flip `committed`; keep the registration permanently
    ///
    /// CREATE / ATTACH path. These callers do not pass a scope pointer explicitly: the inline
    /// `disk(...)` conversion happens deep inside `StorageFactory` / `DatabaseFactory` via
    /// `MergeTreeSettings::loadFromQuery` / `DatabaseMetadataDiskSettings::loadFromQuery`, which
    /// have no scope parameter. Instead, `InterpreterCreateQuery` installs an AMBIENT scope on the
    /// current thread for the duration of `doCreateTable` / `createDatabase`:
    ///
    ///     CustomDiskRegistrationScope create_disk_scope(getContext(), /*install_as_ambient_create_scope=*/true);
    ///     // ... StorageFactory::get -> loadFromQuery -> getOrCreateCustomDisk picks up the
    ///     //     ambient scope and tracks any freshly-registered disk under it ...
    ///     // ... validateStorage, replicated fault injection, database->createTable / writeMetadataFile ...
    ///     create_disk_scope.commit();   /// only after the metadata transition is durable
    ///
    /// If any step before `commit()` throws, the destructor rolls the freshly-registered disk back,
    /// mirroring the ALTER caller-owned scope. See issue #63019 and PR #103818.
    class CustomDiskRegistrationScope
    {
    public:
        /// `install_as_ambient_create_scope` makes this scope the current thread's ambient scope
        /// for unscoped CREATE / ATTACH disk conversions (see class comment). The previous ambient
        /// scope (if any, e.g. a nested CREATE OR REPLACE) is saved and restored on destruction.
        explicit CustomDiskRegistrationScope(ContextPtr context_, bool install_as_ambient_create_scope = false);
        ~CustomDiskRegistrationScope() noexcept;

        CustomDiskRegistrationScope(const CustomDiskRegistrationScope &) = delete;
        CustomDiskRegistrationScope & operator=(const CustomDiskRegistrationScope &) = delete;

        /// Record that this scope freshly registered a custom disk by this name.
        void track(const std::string & disk_name);

        /// Mark the scope as committed and clear all pending-rollback markers we own.
        void commit() noexcept;

    private:
        ContextPtr context;
        std::vector<std::string> registered_disk_names;
        bool committed = false;
        bool installed_as_ambient = false;
        CustomDiskRegistrationScope * previous_ambient_scope = nullptr;
    };

    /// The current thread's ambient CREATE / ATTACH scope, or nullptr. Set by
    /// `CustomDiskRegistrationScope` when constructed with `install_as_ambient_create_scope = true`.
    /// Used by `getOrCreateCustomDisk` to track unscoped registrations so they can be rolled back
    /// if the outer CREATE / ATTACH fails before its metadata transition is durable.
    CustomDiskRegistrationScope * currentAmbientCreateScope();

    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    std::string createCustomDisk(const ASTPtr & disk_function, ContextPtr context, bool attach, CustomDiskRegistrationScope * scope = nullptr);

    /// If `value` is a `CustomType` wrapping a `disk(...)` function AST (as produced by the parser
    /// for `disk = disk(...)`), register the custom disk and replace `value` in-place with the
    /// resulting disk name string. Otherwise (`value` is already a String), validate that the
    /// referenced disk is not a custom disk used by another table.
    ///
    /// If `scope` is non-null and a NEW custom disk is registered, the disk's name is recorded in
    /// the scope's pending-rollback list. The actual rollback is conditional on no other DDL having
    /// observed the registration in the meantime; see `CustomDiskRegistrationScope`.
    ///
    /// This must be called for the `disk` setting before passing it to `BaseSettings::applyChanges`
    /// -`SettingFieldString::operator=` calls `Field::safeGet<String>` and throws `BAD_GET` when
    /// the field is a `CustomType`.
    void convertCustomDiskField(Field & value, ContextPtr context, bool attach, CustomDiskRegistrationScope * scope = nullptr);

    /// Walk a `SettingsChanges` vector and call `convertCustomDiskField` on the value of every
    /// change named `disk`. Used by all code paths that re-apply a table's `settings_changes` AST
    /// (CREATE, ALTER, validation) -without this conversion, any setting carrying an inline
    /// `disk(...)` function trips `Field::safeGet<String>` with `BAD_GET`.
    void convertCustomDiskSettings(SettingsChanges & changes, ContextPtr context, bool attach, CustomDiskRegistrationScope * scope = nullptr);
}

}
