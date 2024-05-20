#pragma once

#include <string>
#include <base/StringRef.h>

namespace DB
{

StringRef parentNodePath(StringRef path);

StringRef getBaseNodeName(StringRef path);

}
