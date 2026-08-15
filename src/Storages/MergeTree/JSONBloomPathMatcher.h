#pragma once

#include <Common/re2.h>
#include <Core/Types.h>

#include <list>
#include <string_view>
#include <vector>

namespace DB
{

class JSONBloomPathMatcher
{
public:
    JSONBloomPathMatcher(
        std::vector<String> include_paths_,
        const std::vector<String> & include_path_regexps_,
        std::vector<String> skip_paths_,
        const std::vector<String> & skip_path_regexps_);

    bool shouldIndex(std::string_view path) const;
    bool shouldVisit(std::string_view path) const;

private:
    using Regexps = std::list<re2::RE2>;

    bool hasIncludeFilter() const { return !include_paths.empty() || !include_regexps.empty(); }
    static std::vector<String> normalizePaths(std::vector<String> paths);
    static bool matchesAnyPathOrSubtree(std::string_view path, const std::vector<String> & paths);
    static bool matchesAnyAncestor(std::string_view path, const std::vector<String> & paths);
    static bool matchesAnyRegexp(std::string_view path, const Regexps & regexps);
    static void compileRegexps(const std::vector<String> & regexp_strings, Regexps & regexps);

    std::vector<String> include_paths;
    std::vector<String> skip_paths;
    Regexps include_regexps;
    Regexps skip_regexps;
};

}
