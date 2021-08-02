#pragma once

#include <Core/Types.h>


namespace DB
{

/// Outputs built-in or custom setting's name.
/// The function is like backQuoteIfNeed() but didn't quote with backticks
/// if the name consists of identifiers joined with dots.
void formatSettingName(const String & setting_name, std::ostream & out);

}
