#pragma once
#include <string>

namespace DB
{
/* Parse globs in string and make a regexp for it.
 */
std::string makeRegexpPatternFromGlobs(const std::string & ipath);
}
