#include <Interpreters/checkValuelessSettingChanges.h>

#include <Common/Exception.h>
#include <Core/BaseSettings.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void checkValuelessSettingChanges(const IAST & ast)
{
    const auto check = [](const SettingsChanges & changes)
    {
        for (const auto & change : changes)
            if (change.shorthand && change.value != Field(true))
                BaseSettingsHelpers::throwValuelessSettingHasValue(change.name);
    };

    /// Dictionary and WASM function settings mandate `name = value` in their grammar, so the
    /// valueless form itself is parser-impossible there, whatever the value. It cannot be let
    /// through even with the mandatory `true`: `ASTCreateWasmFunctionQuery::formatImpl` renders
    /// its settings via a temporary `ASTSetQuery`, which elides `= true` for the valueless form,
    /// so a surviving flag would be persisted as `SETTINGS name` that the SQL grammar then fails
    /// to parse back when the function is reloaded.
    const auto check_no_shorthand = [](const SettingsChanges & changes)
    {
        for (const auto & change : changes)
            if (change.shorthand)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Setting '{}' is marked as written without a value, "
                    "but the valueless form is not part of the syntax here. Write '{} = <value>'",
                    change.name, change.name);
    };

    const auto check_no_shorthand_in_set_query = [&](const ASTPtr & settings_ast)
    {
        if (!settings_ast)
            return;
        if (const auto * set_query = settings_ast->as<ASTSetQuery>())
            check_no_shorthand(set_query->changes);
    };

    /// Most settings ride in an `ASTSetQuery`, but dictionary and WASM function settings
    /// store `SettingsChanges` directly in their own nodes, and their `readJSON` restores
    /// the `shorthand` flag just like `ASTSetQuery::readJSON` does.
    if (const auto * set_query = ast.as<ASTSetQuery>())
        check(set_query->changes);
    else if (const auto * dictionary_settings = ast.as<ASTDictionarySettings>())
        check_no_shorthand(dictionary_settings->changes);
    else if (const auto * create_wasm_function = ast.as<ASTCreateWasmFunctionQuery>())
        check_no_shorthand(create_wasm_function->getSettings());
    /// Some settings do ride in an `ASTSetQuery`, but their grammar disables the valueless form
    /// (`ParserSetQuery` with `shorthand_syntax = false`), so the flag is parser-impossible there
    /// whatever the value, and the node cannot tell in which grammar it was parsed - the context
    /// has to be judged from the parent. It cannot be let through even with the mandatory `true`:
    /// nothing in these paths consults the flag, and `ASTSetQuery::formatImpl` elides `= true` for
    /// the valueless form, so a surviving flag would be persisted (e.g. in the column declaration
    /// of a table definition) as `SETTINGS name` that these grammars then fail to parse back.
    else if (const auto * column_declaration = ast.as<ASTColumnDeclaration>())
        check_no_shorthand_in_set_query(column_declaration->getSettings());
    else if (const auto * explain_query = ast.as<ASTExplainQuery>())
        check_no_shorthand_in_set_query(explain_query->getSettings());
    /// `BACKUP`/`RESTORE` settings also ride in an `ASTSetQuery`, but `ParserBackupQuery` accepts
    /// only `name = value` pairs (`ParserSetQuery::parseNameValuePair`), so the valueless form is
    /// parser-impossible there too. It is not just a formatting hazard: `BackupSettings` and
    /// `RestoreSettings` consume `setting.value` raw, and numeric settings such as
    /// `compression_level` would coerce the mandatory `Bool(true)` to a real value.
    else if (const auto * backup_query = ast.as<ASTBackupQuery>())
        check_no_shorthand_in_set_query(backup_query->settings);

    for (const auto & child : ast.children)
        checkValuelessSettingChanges(*child);
}

}
