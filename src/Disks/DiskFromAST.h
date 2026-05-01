#pragma once
#include <string>
#include <Common/SettingsChanges.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    std::string createCustomDisk(const ASTPtr & disk_function, ContextPtr context, bool attach);

    /// If `value` is a `CustomType` wrapping a `disk(...)` function AST (as produced by the parser
    /// for `disk = disk(...)`), register the custom disk and replace `value` in-place with the
    /// resulting disk name string. Otherwise (`value` is already a String), validate that the
    /// referenced disk is not a custom disk used by another table.
    ///
    /// This must be called for the `disk` setting before passing it to `BaseSettings::applyChanges`
    /// — `SettingFieldString::operator=` calls `Field::safeGet<String>` and throws `BAD_GET` when
    /// the field is a `CustomType`.
    void convertCustomDiskField(Field & value, ContextPtr context, bool attach);

    /// Walk a `SettingsChanges` vector and call `convertCustomDiskField` on the value of every
    /// change named `disk`. Used by all code paths that re-apply a table's `settings_changes` AST
    /// (CREATE, ALTER, validation) — without this conversion, any setting carrying an inline
    /// `disk(...)` function trips `Field::safeGet<String>` with `BAD_GET`.
    void convertCustomDiskSettings(SettingsChanges & changes, ContextPtr context, bool attach);
}

}
