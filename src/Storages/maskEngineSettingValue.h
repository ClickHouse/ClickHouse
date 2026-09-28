#pragma once

#include <base/types.h>

namespace DB
{

class Field;
struct SettingDescription;

/// Returns `value` - a table engine setting's rendered text - with its credential hidden, or an empty string when it
/// holds none. The system table counterpart of `CoreSettings::maskSettingValue` and `renderSecretChangeValue` in
/// `ASTSetQuery.cpp`: a name is masked whichever engine registered it.
String maskEngineSettingValue(const String & setting_name, const Field & field, const String & value);

/// The same for `setting.value`, asked under every name the setting answers to: a registry keys its rules by the
/// names its engine happened to list, and `SHOW CREATE TABLE` hides a secret under whichever of them a clause states.
String maskEngineSettingValue(const SettingDescription & setting, const Field & field);

}
