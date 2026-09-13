#pragma once

#include <base/types.h>

namespace DB
{

class Field;

/// Returns `value` - a table engine setting's already-rendered text - with the credential it holds
/// hidden, or an empty string when it holds none.
///
/// The counterpart of `CoreSettings::maskSettingValue`, which does this for the query-level
/// `Settings` collection, and the sibling of `ASTSetQuery`'s `renderSecretChangeValue`, which does it
/// for a sink that prints SQL and so needs the result quoted. This is the form for a sink that prints
/// the value as it is - a system table column.
///
/// Like both of those it ignores which engine the table has. `kafka_sasl_password` is a credential
/// wherever it appears, and a name that belongs to no engine's registry is masked by none of them.
String maskEngineSettingValue(const String & setting_name, const Field & field, const String & value);

}
