#pragma once

#include <base/types.h>

namespace DB
{

class Field;

/// Returns `value` - a table engine setting's already-rendered text - with the credential it holds
/// hidden, or an empty string when it holds none.
///
/// The system table counterpart of `CoreSettings::maskSettingValue` and of `renderSecretChangeValue` in
/// `ASTSetQuery.cpp`, which renders quoted SQL. Like them it ignores the table's engine: a name is masked
/// wherever it appears, whichever engine registered it.
String maskEngineSettingValue(const String & setting_name, const Field & field, const String & value);

}
