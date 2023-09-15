#pragma once

#include <string>
#include <base/StringRef.h>

namespace DB
{

StringRef parentPath(StringRef path);

StringRef getBaseName(StringRef path);

}
