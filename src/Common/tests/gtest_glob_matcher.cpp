#include <Common/Exception.h>
#include <Common/GlobMatcher.h>
#include <Core/Settings.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPILE_REGEXP;
}

TEST(GlobMatcher, EngineFromSettings)
{
    Settings settings;
    EXPECT_EQ(GlobMatcher::chooseEngine(settings), GlobMatcher::Engine::Regexp);

    settings.set("use_regexp_free_glob_matcher", true);
    EXPECT_EQ(GlobMatcher::chooseEngine(settings), GlobMatcher::Engine::Glob);

    /// The matcher made from the settings is the one of the engine they choose: only `Glob` takes this range.
    EXPECT_TRUE(GlobMatcher("f{0..1000000}", settings).matches("f999999"));
}

TEST(GlobMatcher, EnginesAgree)
{
    const std::vector<std::string> patterns = {
        "data/*.csv", "data/**", "**/x", "f{1..10}", "f{01..10}", "{a,b*}/?", "{a}", "{-}", "^a$", "a$b"};
    const std::vector<std::string> candidates = {
        "data/a.csv", "data/a/b.csv", "x", "a/b/x", "f7", "f07", "f10", "a/c", "bcd/e", "{a}", "-", "a", "a$b", ""};

    for (const auto & pattern : patterns)
    {
        const GlobMatcher regexp(pattern, GlobMatcher::Engine::Regexp);
        const GlobMatcher glob(pattern, GlobMatcher::Engine::Glob);

        for (const auto & candidate : candidates)
            EXPECT_EQ(regexp.matches(candidate), glob.matches(candidate)) << "pattern '" << pattern << "', candidate '" << candidate << "'";
    }
}

TEST(GlobMatcher, PatternsOnlyOneEngineTakes)
{
    auto error_code = [](const std::string & pattern, GlobMatcher::Engine engine)
    {
        try
        {
            GlobMatcher matcher(pattern, engine);
        }
        catch (const Exception & e)
        {
            return e.code();
        }
        return 0;
    };

    /// A range of more than 100000 numbers.
    EXPECT_EQ(error_code("f{0..1000000}", GlobMatcher::Engine::Regexp), ErrorCodes::BAD_ARGUMENTS);
    EXPECT_TRUE(GlobMatcher("f{0..1000000}", GlobMatcher::Engine::Glob).matches("f999999"));

    /// A pattern that is not valid UTF-8.
    EXPECT_EQ(error_code("a\xFF*", GlobMatcher::Engine::Regexp), ErrorCodes::CANNOT_COMPILE_REGEXP);
    EXPECT_TRUE(GlobMatcher("a\xFF*", GlobMatcher::Engine::Glob).matches("a\xFF" "b"));

    /// A range with a bound that does not fit into 64 bits.
    EXPECT_EQ(error_code("f{99999999999999999999..99999999999999999999}", GlobMatcher::Engine::Regexp), 0);
    EXPECT_EQ(error_code("f{99999999999999999999..99999999999999999999}", GlobMatcher::Engine::Glob), ErrorCodes::BAD_ARGUMENTS);
}

TEST(GlobMatcher, Move)
{
    for (const auto engine : {GlobMatcher::Engine::Regexp, GlobMatcher::Engine::Glob})
    {
        GlobMatcher original("{a,bc}{1..3}*", engine);
        GlobMatcher moved = std::move(original);
        EXPECT_TRUE(moved.matches("bc2x"));
        EXPECT_FALSE(moved.matches("b2x"));
    }
}
