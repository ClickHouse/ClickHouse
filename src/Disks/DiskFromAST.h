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
    /// RAII guard that records custom-disk registrations performed during ALTER validation
    /// (or any other dry-run that calls `convertCustomDiskField`/`convertCustomDiskSettings`)
    /// and rolls them back on destruction unless `commit` was called.
    ///
    /// Usage at a validation call site (e.g. `MergeTreeData::checkAlterIsPossible`):
    ///
    ///     CustomDiskRegistrationScope disk_scope(local_context);
    ///     DiskFromAST::convertCustomDiskSettings(new_changes, local_context, false, &disk_scope);
    ///     // ... validation that may throw ...
    ///     // No `disk_scope.commit();` -destructor rolls back any newly-registered disks.
    ///
    /// Usage at the actual apply site (e.g. `MergeTreeData::changeSettings`):
    ///
    ///     CustomDiskRegistrationScope disk_scope(getContext());
    ///     DiskFromAST::convertCustomDiskSettings(new_changes, getContext(), false, &disk_scope);
    ///     // ... apply work ...
    ///     disk_scope.commit();   /// keep the registrations
    ///
    /// Idempotent: a second call to `convertCustomDiskSettings` for the same inline disk
    /// (already in the disk selector after a prior validation pass) is a no-op for the scope.
    /// Only NEW registrations are tracked. So a validation pass that succeeds and rolls back,
    /// followed by a real apply pass, will correctly re-register the disk in the apply scope.
    class CustomDiskRegistrationScope
    {
    public:
        explicit CustomDiskRegistrationScope(ContextPtr context_);
        ~CustomDiskRegistrationScope() noexcept;

        CustomDiskRegistrationScope(const CustomDiskRegistrationScope &) = delete;
        CustomDiskRegistrationScope & operator=(const CustomDiskRegistrationScope &) = delete;

        /// Record a name newly added to the disk selector during this scope.
        void track(const std::string & disk_name);

        /// Mark the scope as committed; destructor will not roll back.
        void commit() noexcept { committed = true; }

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
    /// If `scope` is non-null and a NEW custom disk is registered, the disk's name is recorded
    /// in the scope so it can be rolled back if the surrounding validation later throws.
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
