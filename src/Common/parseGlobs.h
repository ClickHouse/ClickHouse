#pragma once

#include <optional>
#include <string>
#include <vector>


namespace DB
{
    bool containsRangeGlob(const std::string & input);
    bool containsOnlyEnumGlobs(const std::string & input);
    bool hasExactlyOneBracketsExpansion(const std::string & input);

    /// Whether `expandSelectionGlobFirst` can parse the pattern, i.e. whether its `{a,b,c}` groups
    /// are the ones a selector glob is made of: not nested, closed, and holding every comma.
    /// `makeRegexpPatternFromGlobs` is more permissive - a doubled brace like `{{a,b}}` is a literal
    /// brace around an enum for it, and a comma outside a group is literal text - so a caller that
    /// only wants a sample path has to ask first instead of refusing a path the reader would read.
    bool canExpandSelectionGlobFirst(const std::string & input);

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
    /// product it is always possible whatever the groups multiply out to, where the full expansion
    /// can be refused for a pattern the reader would go on to match as a regexp. It throws for a
    /// pattern `canExpandSelectionGlobFirst` rejects.
    std::string expandSelectionGlobFirst(const std::string & path);

    /// `expandSelectionGlobFirst` for a caller whose reader matches the pattern as a regexp built by
    /// `makeRegexpPatternFromGlobs`: the first path, when `canExpandSelectionGlobFirst` accepts the
    /// pattern and that regexp compiles and matches the path, and nothing otherwise - the caller
    /// then has to list the objects the same way the reader does.
    std::optional<std::string> tryExpandSelectionGlobFirstMatchedByRegexp(const std::string & path);
}
