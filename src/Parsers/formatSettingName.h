#pragma once

#include <iosfwd>
#include <common/types.h>


namespace DB
{

class WriteBuffer;

/// Outputs built-in or custom setting's name.
/// The function is like backQuoteIfNeed() but didn't quote with backticks
/// if the name consists of identifiers joined with dots.
void formatSettingName(const String & setting_name, WriteBuffer & out);

}
