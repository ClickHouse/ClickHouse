#pragma once
#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <base/demangle.h>
#include <Core/Field.h>
#include <Common/AddressToLineCache.h>

#include <vector>

namespace DB
{

std::pair<Array, Array> generateArraysSymbolsLines(const std::vector<UInt64> trace);

}
#endif
