#pragma once

#include <string>
#include <vector>


namespace DB
{
    bool containsRangeGlob(const std::string & input);
    bool containsOnlyEnumGlobs(const std::string & input);
    bool hasExactlyOneBracketsExpansion(const std::string & input);

    /// Parse globs in string and make a regexp for it.
    /// A `{N..M}` range glob becomes an alternation of every number of the range, so it throws
    /// instead of building a regexp for an unreasonably long range, or for unreasonably many of them.
    std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs);

    /// Process {a,b,c...} globs:
    /// Don't match it against regex, but generate a,b,c strings instead and process each of them separately.
    /// E.g. for a string like `file{1,2,3}.csv` return vector of strings: {`file1.csv`,`file2.csv`,`file3.csv`}
    /// The expansion is a Cartesian product of the groups, so it throws instead of expanding a pattern
    /// that asks for an unreasonable number of paths, an unreasonable amount of data, or too many groups.
    std::vector<std::string> expandSelectionGlob(const std::string & path);

    /// The first path `expandSelectionGlob` would return, picking each group's first alternative
    /// without enumerating the rest. For a caller that needs one sample path rather than the whole
    /// product it is also always possible, where the full expansion can be refused for a pattern
    /// the reader would go on to match as a regexp.
    std::string expandSelectionGlobFirst(const std::string & path);
}
