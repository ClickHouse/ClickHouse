#pragma once

namespace DB
{

class IAST;

/// The valueless form `SETTINGS name` stands for `name = true`, and the SQL parser always writes
/// Bool `true` for it, so `shorthand` paired with any other value is a parser-impossible shape that
/// can only arrive from the AST JSON dialect. Where the grammar does not accept the valueless form
/// at all - dictionary, WASM function, column-level and `EXPLAIN` settings - the flag itself is
/// parser-impossible, whatever the value. `BaseSettings::checkShorthandChange` rejects the mismatch
/// for the settings applied through `BaseSettings`, but `SettingsChanges` are also consumed raw -
/// e.g. the `Join` and `Log` engines, `EXPLAIN` settings, dictionary and data-lake settings - and
/// formatting such an AST back to SQL can silently drop the carried value or render text the SQL
/// grammar cannot parse back. Instead of duplicating the check in each consumer, reject the shape
/// once for the whole tree at every `IAST::createFromJSON` entry point.
void checkValuelessSettingChanges(const IAST & ast);

}
