#include <Storages/MergeTree/TextIndexDictionaryAutomaton.h>

#include <re2/re2.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <set>

using namespace DB;

namespace
{

using Automaton = TextIndexDictionaryAutomaton;

std::vector<std::string> intersect(const Automaton & automaton, const std::vector<std::string> & dictionary, size_t * visited = nullptr)
{
    Automaton::Cursor cursor(automaton);
    std::vector<std::string> result;
    std::string target;
    auto it = dictionary.begin();
    while (it != dictionary.end())
    {
        if (visited)
            ++*visited;
        switch (cursor.next(*it, target))
        {
            case Automaton::Result::Match:
                result.push_back(*it++);
                break;
            case Automaton::Result::Seek:
                EXPECT_GT(target, *it);
                it = std::lower_bound(it + 1, dictionary.end(), target);
                break;
            case Automaton::Result::Exhausted:
                return result;
        }
    }
    return result;
}

void compareWithRE2(const std::string & pattern, const std::vector<std::string> & dictionary, const re2::RE2::Options & options = {})
{
    SCOPED_TRACE(pattern);
    re2::RE2 regexp(pattern, options);
    ASSERT_TRUE(regexp.ok());
    auto automaton = Automaton::fromRegexp(regexp, 16 << 20);
    ASSERT_TRUE(automaton);
    std::vector<std::string> expected;
    Automaton::Cursor cursor(*automaton);
    std::string target;
    for (const auto & token : dictionary)
    {
        const bool matches = re2::RE2::PartialMatch(token, regexp);
        if (matches)
            expected.push_back(token);
        EXPECT_EQ(cursor.next(token, target) == Automaton::Result::Match, matches) << token;
    }
    EXPECT_EQ(intersect(*automaton, dictionary), expected);
}

}

TEST(TextIndexDictionaryAutomaton, LiteralAndUnsignedByteOrder)
{
    std::vector<std::string> dictionary{"", "a", "aa", "ab", "b", std::string("a\0", 2), std::string("a\xff", 2), "\xff"};
    std::sort(dictionary.begin(), dictionary.end());
    for (const auto & literal : dictionary)
    {
        EXPECT_EQ(intersect(*Automaton::literal(literal, false), dictionary), std::vector<std::string>{literal});
        std::vector<std::string> expected;
        std::copy_if(dictionary.begin(), dictionary.end(), std::back_inserter(expected),
            [&](const auto & token) { return token.starts_with(literal); });
        EXPECT_EQ(intersect(*Automaton::literal(literal, true), dictionary), expected);
    }
}

TEST(TextIndexDictionaryAutomaton, RegexSemanticsAndSeekSoundness)
{
    std::set<std::string> words{"", "a", "ab", "abc", "bar", "foo", "foobar", "FOO", "food", "\nfoo\n", "é", "éclair", "Ж", "K", "k"};
    std::mt19937 random(123);
    const std::string alphabet = std::string("abfoxyz012 \n_") + '\0' + '\xff';
    for (size_t i = 0; i < 3000; ++i)
    {
        std::string word;
        size_t length = random() % 16;
        while (word.size() < length)
            word.push_back(alphabet[random() % alphabet.size()]);
        words.insert(std::move(word));
    }
    std::vector<std::string> dictionary(words.begin(), words.end());
    for (const auto * pattern : {"", "a", "^a", "a$", "^a$", "a.*b", "^(foo|bar)[a-z]*$", "^a*b$", "^a?b$",
             "^a{1,3}b$", "(?m)^foo$", "\\bfoo\\b", "\\Bfoo", "(?i)^foo", "(?i)^k$", "^[^a-z]*$", "^é.*$",
             "\\p{L}+", "^\\C*$", "^a\\x00.*$", "(?s)^.*$", "^$", "a^"})
        compareWithRE2(pattern, dictionary);

    re2::RE2::Options latin1;
    latin1.set_encoding(re2::RE2::Options::EncodingLatin1);
    compareWithRE2("^\xff.*$", dictionary, latin1);

    re2::RE2::Options like_options;
    like_options.set_dot_nl(true);
    compareWithRE2("^foo.*$", dictionary, like_options);
    like_options.set_case_sensitive(false);
    compareWithRE2("^foo.*$", dictionary, like_options);
    compareWithRE2("^k.*$", dictionary, like_options);
}

TEST(TextIndexDictionaryAutomaton, SkipsRejectedSubtrees)
{
    std::vector<std::string> dictionary;
    for (size_t i = 0; i < 10000; ++i)
        dictionary.push_back("a" + std::to_string(i));
    dictionary.insert(dictionary.end(), {"serviceaerror", "serviceberror", "servicezok", "zzz"});
    std::sort(dictionary.begin(), dictionary.end());
    re2::RE2 regexp("^service[a-b]error$");
    auto automaton = Automaton::fromRegexp(regexp);
    ASSERT_TRUE(automaton);
    EXPECT_TRUE(automaton->canSkipPrefixes());
    size_t visited = 0;
    EXPECT_EQ(intersect(*automaton, dictionary, &visited), (std::vector<std::string>{"serviceaerror", "serviceberror"}));
    EXPECT_LT(visited, 20);
}

TEST(TextIndexDictionaryAutomaton, CyclicLanguageAndDeadStates)
{
    /// This byte DFA accepts a*b. State 2 is a non-accepting cycle and must be
    /// removed from seek targets even though it has outgoing transitions.
    Automaton automaton({{false, {{'a', 'a', 0}, {'b', 'b', 1}, {'c', 'c', 2}}}, {true, {}}, {false, {{0, 255, 2}}}});
    std::vector<std::string> dictionary{"", "a", "aa", "aaa", "aaaaab", "ab", "ac", "b", "c", "cc"};
    EXPECT_EQ(intersect(automaton, dictionary), (std::vector<std::string>{"aaaaab", "ab", "b"}));
    Automaton empty({{false, {{0, 255, 0}}}});
    EXPECT_TRUE(intersect(empty, dictionary).empty());
}

TEST(TextIndexDictionaryAutomaton, DeterminizationBudget)
{
    re2::RE2 regexp("^[ab]*a[ab]{15}$");
    EXPECT_FALSE(Automaton::fromRegexp(regexp, 0));
    EXPECT_FALSE(Automaton::fromRegexp(regexp, 1024));
    EXPECT_TRUE(Automaton::fromRegexp(re2::RE2("^foo.*bar$")));
}
