#pragma once
#include <string>
#include <vector>

namespace DB
{
    /// Parse globs in string and make a regexp for it.
    std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs);

    /// Process {a,b,c...} globs:
    /// Don't match it against regex, but generate a,b,c strings instead and process each of them separately.
    /// E.g. for a string like `file{1,2,3}.csv` return vector of strings: {`file1.csv`,`file2.csv`,`file3.csv`}
    std::vector<std::string> expandSelectionGlob(const std::string & path);
}
