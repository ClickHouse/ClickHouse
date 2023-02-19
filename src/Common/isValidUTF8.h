#pragma once

#include <base/types.h>

namespace DB::UTF8
{

UInt8 isValidUTF8(const UInt8 * data, UInt64 len);

}
