#pragma once

#include <string>
#include <base/StringRef.h>

namespace DB
{

std::string parentPath(StringRef path);

std::string getBaseName(StringRef path);

}
