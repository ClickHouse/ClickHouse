#pragma once

#include <base/types.h>

#include <functional>
#include <optional>
#include <span>
#include <unordered_map>

namespace DB
{

class Field;

/// Each engine namespace declares its own identical `ValueMaskingFunc` alias, hence the spelled-out
/// type. Unrelated to `CoreSettings::ValueMaskingFunc`, which rewrites a value string in place.
using EngineSettingsToHide = std::unordered_map<String, std::function<std::optional<std::string>(const Field &)>>;

/// The table and database engine settings whose value is a secret, and how each one is masked - one
/// map per engine family. `ASTSetQuery`, which prints these settings as SQL, and
/// `maskEngineSettingValue`, which prints them in a system table, both read this list, so the two
/// cannot disagree on what is secret.
std::span<const EngineSettingsToHide * const> engineSettingsToHide();

}
