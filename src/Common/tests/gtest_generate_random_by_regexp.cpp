#include <Common/MatchGenerator.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

void routine(String s)
{
    std::cerr << "case '"<< s << "'";
    auto gen = DB::RandomStringGeneratorByRegexp(s);
    [[maybe_unused]] auto res = gen.generate();
    std::cerr << " result '"<< res << "'" << std::endl;
}

TEST(GenerateRandomString, Positive)
{
    routine(".");
    routine("[[:xdigit:]]");
    routine("[0-9a-f]");
    routine("[a-z]");
    routine("prefix-[0-9a-f]-suffix");
    routine("prefix-[a-z]-suffix");
    routine("[0-9a-f]{3}");
    routine("prefix-[0-9a-f]{3}-suffix");
    routine("prefix-[a-z]{3}-suffix/[0-9a-f]{20}");
    routine("left|right");
    routine("[a-z]{0,3}");
    routine("just constant string");
    routine("[a-z]?");
    routine("[a-z]*");
    routine("[a-z]+");
    routine("[^a-z]");
    routine("[[:lower:]]{3}/suffix");
    routine("prefix-(A|B|[0-9a-f]){3}");
    routine("mergetree/[a-z]{3}/[a-z]{29}");
}

TEST(GenerateRandomString, Negative)
{
    EXPECT_THROW(routine("[[:do_not_exists:]]"), DB::Exception);
    EXPECT_THROW(routine("[:do_not_exis..."), DB::Exception);
    EXPECT_THROW(routine("^abc"), DB::Exception);
}

TEST(GenerateRandomString, DifferentResult)
{
    std::cerr << "100 different keys" << std::endl;
    auto gen = DB::RandomStringGeneratorByRegexp("prefix-[a-z]{3}-suffix/[0-9a-f]{20}");
    std::set<String> deduplicate;
    for (int i = 0; i < 100; ++i)
        ASSERT_TRUE(deduplicate.insert(gen.generate()).second);
    std::cerr << "100 different keys: ok" << std::endl;
}

TEST(GenerateRandomString, FullRange)
{
    std::cerr << "all possible letters" << std::endl;
    auto gen = DB::RandomStringGeneratorByRegexp("[a-z]");
    std::set<String> deduplicate;
    int count = 'z' - 'a' + 1;
    while (deduplicate.size() < count)
        if (deduplicate.insert(gen.generate()).second)
            std::cerr << " +1 ";
    std::cerr << "all possible letters, ok" << std::endl;
}
