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
    /// A registration is only rolled back if no other code path observed it after this scope wrote
    /// it: every successful `Context::getOrCreateDisk` call for an existing custom disk clears the
    /// pending-rollback marker for that name (the caller may now commit metadata against this disk,
    /// so the original validation scope must not delete it). On scope destruction we ask the global
    /// pending-rollback table whether our marker is still present; if yes, the registration is
    /// uniquely ours and we remove the disk; if no, another DDL has observed it and we leak the
    /// name (safe: the disk stays, the existing settings-hash check protects against redefinition).
    ///
    /// Usage at a validation call site (e.g. `MergeTreeData::checkAlterIsPossible`):
    ///
    ///     CustomDiskRegistrationScope disk_scope(local_context);
    ///     DiskFromAST::convertCustomDiskSettings(new_changes, local_context, false, &disk_scope);
    ///     // ... validation that may throw ...
    ///     // No `disk_scope.commit();` -destructor releases markers and rolls back unobserved ones.
    ///
    /// Usage at the actual apply site (e.g. `MergeTreeData::changeSettings`):
    ///
    ///     CustomDiskRegistrationScope disk_scope(getContext());
    ///     DiskFromAST::convertCustomDiskSettings(new_changes, getContext(), false, &disk_scope);
    ///     // ... apply work ...
    ///     disk_scope.commit();   /// keep the registrations; clear all pending-rollback markers
    class CustomDiskRegistrationScope
    {
    public:
        explicit CustomDiskRegistrationScope(ContextPtr context_);
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
    };

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
