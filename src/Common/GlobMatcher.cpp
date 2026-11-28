#include <Common/GlobMatcher.h>

#include <Common/Exception.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace Setting
{
    extern const SettingsBool use_regexp_free_glob_matcher;
}

GlobMatcher::Engine GlobMatcher::chooseEngine(const Settings & settings)
{
    return settings[Setting::use_regexp_free_glob_matcher] ? Engine::Glob : Engine::Regexp;
}

GlobMatcher::GlobMatcher(const std::string & pattern, Engine engine)
{
    if (engine == Engine::Glob)
    {
        glob.emplace(pattern);
        return;
    }

    regexp = std::make_unique<const re2::RE2>(makeRegexpPatternFromGlobs(pattern));
    if (!regexp->ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                        "Cannot compile regex from glob ({}): {}", pattern, regexp->error());
}

GlobMatcher::GlobMatcher(const std::string & pattern, const Settings & settings)
    : GlobMatcher(pattern, chooseEngine(settings))
{
}

GlobMatcher::GlobMatcher(GlobMatcher &&) noexcept = default;
GlobMatcher & GlobMatcher::operator=(GlobMatcher &&) noexcept = default;
GlobMatcher::~GlobMatcher() = default;

bool GlobMatcher::matches(std::string_view candidate) const
{
    if (glob)
        return glob->matches(candidate);
    return re2::RE2::FullMatch(candidate, *regexp);
}

}
