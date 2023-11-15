#pragma once
#include <string>
#include <vector>

namespace DB
{
    /// Parse globs in string and make a regexp for it.
    std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs);

    /// Process {a,b,c...} globs separately: don't match it against regex, but generate a,b,c strings instead.
    void expandSelector(const std::string & path, std::vector<std::string> & for_match_paths_expanded);
}
