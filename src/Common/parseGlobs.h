#pragma once

#include <string>
#include <vector>


namespace DB
{
    bool containsRangeGlob(const std::string & input);
    bool containsOnlyEnumGlobs(const std::string & input);
    bool hasExactlyOneBracketsExpansion(const std::string & input);

    /// Parse globs in string and make a regexp for it.
    std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs);

    /// Process {a,b,c...} globs:
    /// Don't match it against regex, but generate a,b,c strings instead and process each of them separately.
    /// E.g. for a string like `file{1,2,3}.csv` return vector of strings: {`file1.csv`,`file2.csv`,`file3.csv`}
    /// The expansion is a Cartesian product of the groups, so it throws instead of expanding a pattern
    /// that asks for an unreasonable number of paths, an unreasonable amount of data, or too many groups.
    std::vector<std::string> expandSelectionGlob(const std::string & path);
}
