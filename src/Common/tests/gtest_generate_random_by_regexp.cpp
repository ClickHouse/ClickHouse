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

UInt64 elapsed(DB::ObjectStorageKeysGeneratorPtr generator)
{
    String path = "some_path";

    Stopwatch watch;

    for (int i = 0; i < 100000; ++i)
    {
        [[ maybe_unused ]] auto result = generator->generate(path).serialize();
    }

    return watch.elapsedMicroseconds();
}

TEST(ObjectStorageKey, Performance)
{
    auto elapsed_old = elapsed(DB::createObjectStorageKeysGeneratorByPrefix(
            "xx-xx-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/mergetree/"));
    std::cerr << "old: " << elapsed_old << std::endl;

    auto elapsed_new = elapsed(DB::createObjectStorageKeysGeneratorByTemplate(
            "xx-xx-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/mergetree/[a-z]{3}/[a-z]{29}"));
    std::cerr << "new: " << elapsed_new << std::endl;

    if (elapsed_new > elapsed_old)
    {
        if (elapsed_new > elapsed_old)
            std::cerr << "slow ratio: +" << float(elapsed_new) / elapsed_old << std::endl;
        else
            std::cerr << "fast ratio: " << float(elapsed_old) / elapsed_new << std::endl;
        ASSERT_LT(elapsed_new, 1.5 * elapsed_old);
    }

}
