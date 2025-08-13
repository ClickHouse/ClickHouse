#pragma once

#include <base/types.h>

namespace DB::ASCII
{

UInt8 isValidASCII(const UInt8 * data, UInt64 len);

}
