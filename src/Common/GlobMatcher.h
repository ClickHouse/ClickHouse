#pragma once

#include <Common/Glob.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>


namespace re2
{
class RE2;
}

namespace DB
{

struct Settings;

/// Matches paths against a pattern with globs in one of two ways. Both give the same answer for every
/// pattern the first of them accepts, see `Glob`, so which one is used does not change what a path means.
class GlobMatcher
{
public:
    enum class Engine : UInt8
    {
        /// The regexp `makeRegexpPatternFromGlobs` builds from the pattern, matched by `re2`.
        Regexp,
        /// `Glob`, which matches the pattern as it is.
        Glob,
    };

    /// The engine the `use_regexp_free_glob_matcher` setting asks for.
    static Engine chooseEngine(const Settings & settings);

    /// Throws for a pattern the engine refuses: `BAD_ARGUMENTS`, or `CANNOT_COMPILE_REGEXP`
    /// when `re2` does not compile the regexp.
    GlobMatcher(const std::string & pattern, Engine engine);
    GlobMatcher(const std::string & pattern, const Settings & settings);

    GlobMatcher(GlobMatcher &&) noexcept;
    GlobMatcher & operator=(GlobMatcher &&) noexcept;
    ~GlobMatcher();

    /// Whether the whole `candidate` matches the pattern. Can be called from many threads.
    [[nodiscard]] bool matches(std::string_view candidate) const;

private:
    /// Exactly one of the two is set.
    std::optional<Glob> glob;
    std::unique_ptr<const re2::RE2> regexp;
};

}
