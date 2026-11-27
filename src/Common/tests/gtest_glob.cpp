#include <Common/Exception.h>
#include <Common/Glob.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// The regexp `makeRegexpPatternFromGlobs` builds for a pattern: what `Glob` has to agree with.
class RegexpOracle
{
public:
    explicit RegexpOracle(const std::string & pattern)
    {
        std::string regexp;
        try
        {
            regexp = makeRegexpPatternFromGlobs(pattern);
        }
        catch (const Exception &)
        {
            return;
        }

        re2::RE2::Options options;
        options.set_log_errors(false);
        auto compiled = std::make_unique<re2::RE2>(regexp, options);
        if (compiled->ok())
            matcher = std::move(compiled);
    }

    /// False when the pattern is refused, or is turned into a regexp `re2` does not compile.
    [[nodiscard]] bool exists() const { return matcher != nullptr; }

    [[nodiscard]] bool matches(const std::string & candidate) const { return re2::RE2::FullMatch(candidate, *matcher); }

private:
    std::unique_ptr<re2::RE2> matcher;
};

struct Case
{
    std::string pattern;
    std::string candidate;
    bool matches;
};

/// Every case is checked against the regexp too, so that a case which is wrong about the language
/// fails as well as a wrong `Glob` does.
void check(const std::vector<Case> & cases)
{
    for (const auto & [pattern, candidate, matches] : cases)
    {
        SCOPED_TRACE("pattern '" + pattern + "', candidate '" + candidate + "'");

        EXPECT_EQ(Glob(pattern).matches(candidate), matches);

        RegexpOracle oracle(pattern);
        ASSERT_TRUE(oracle.exists());
        EXPECT_EQ(oracle.matches(candidate), matches);
    }
}

}


TEST(Glob, Wildcards)
{
    check({
        {"", "", true},
        {"", "a", false},
        {"file.csv", "file.csv", true},
        {"file.csv", "fileXcsv", false},
        {"file.csv", "file.csv.gz", false},

        {"?", "a", true},
        {"?", "", false},
        {"?", "ab", false},
        {"?", "/", false},
        {"a?c", "abc", true},

        {"*", "", true},
        {"*", "abc", true},
        {"*", "a/b", false},
        {"*.csv", "a.csv", true},
        {"*.csv", "dir/a.csv", false},
        {"dir/*.csv", "dir/a.csv", true},
        {"a*b*c", "abc", true},
        {"a*b*c", "aXbYc", true},
        {"a*b*c", "acb", false},
    });
}

TEST(Glob, DoubleStar)
{
    check({
        /// The second `*` of two matches '/', but not a brace.
        {"data/**", "data/a/b.csv", true},
        {"data/**", "data/a{b}", true},
        {"data/**", "data/a/{b}", false},
        {"data/**.csv", "data/a/b.csv", true},
        {"***", "a/b", true},

        /// A `?` between the two does not separate them.
        {"a*?*b", "ax/y/b", true},
        {"a*?b", "ax/yb", false},

        /// `**/` as a whole path component matches any number of directories.
        {"**/file.txt", "file.txt", true},
        {"**/file.txt", "a/b/file.txt", true},
        {"**/file.txt", "afile.txt", false},
        {"data/**/x", "data/x", true},
        {"data/**/x", "data/a/b/x", true},
        {"data/**/x", "data/ax", false},
        {"data/**/**/x", "data/x", true},

        /// Otherwise it is two stars and a '/'.
        {"a**/x", "a/b/x", true},
        {"a**/x", "ax", false},
        {"***/x", "x", false},
        {"{a,b}**/x", "a/x", true},
        {"{a,b}**/x", "ax", false},
        {"{**/a,b}", "x/y/a", true},
        {"{**/a,b}", "a", false},
        {"x/{y/**/a,b}", "x/y/a", true},
    });
}

TEST(Glob, Enums)
{
    check({
        {"{a,b}", "a", true},
        {"{a,b}", "b", true},
        {"{a,b}", "c", false},
        {"{a,b}", "{a,b}", false},
        {"x{a,b}y{c,d}z", "xbycz", true},
        {"{ab}", "ab", true},
        {"{ab}", "{ab}", false},
        {"{a.b,c}", "a.b", true},
        {"{a.b,c}", "axb", false},

        /// A body of one character is not an enum...
        {"{a}", "{a}", true},
        {"{a}", "a", false},
        {"{é}", "{é}", true},
        {"{é}", "é", false},
        /// ... unless it is one of the characters escaped before the enums are looked for.
        {"{-}", "-", true},
        {"{-}", "{-}", false},
        {"{+}", "+", true},
        {"{\\}", "\\", true},

        /// Neither is a body that is empty, or starts or ends with a comma.
        {"{}", "{}", true},
        {"{,a}", "{,a}", true},
        {"{,a}", "a", false},
        {"{a,}", "{a,}", true},
        /// An empty alternative in the middle is fine.
        {"x{a,,b}", "x", true},
        {"x{a,,b}", "xa", true},

        /// A body does not hold a brace.
        {"{{a,b}}", "{a}", true},
        {"{{a,b}}", "a", false},
        {"{a{b,c}", "{ab", true},
        {"{a{b,c}", "ab", false},
        {"{a,b}}", "a}", true},

        /// Wildcards work inside the alternatives.
        {"{a,b*}", "bxyz", true},
        {"{a,b*}", "b/x", false},
        {"{a?,b}.csv", "ax.csv", true},
        {"*.{csv,csv.*}", "a.csv", true},
        {"*.{csv,csv.*}", "a.csv.gz", true},
        {"*.{csv,csv.*}", "a.csvx", false},

        /// The same alternative twice, and one that is a prefix of another.
        {"{a,a,ab}c", "abc", true},
        {"{a,a,ab}c", "ac", true},
        {"{a,a,ab}c", "abbc", false},
    });
}

TEST(Glob, Ranges)
{
    check({
        {"f{1..9}", "f5", true},
        {"f{1..9}", "f10", false},
        {"f{1..9}", "f05", false},
        {"f{1..9}", "f", false},
        {"f{20..15}", "f17", true},
        {"f{20..15}", "f21", false},
        {"f{5..5}", "f5", true},

        /// A single '0' does not ask for padding.
        {"f{0..10}", "f0", true},
        {"f{0..10}", "f10", true},
        {"f{0..10}", "f00", false},
        {"f{0..10}", "f010", false},

        /// Padding comes from the smaller bound, to the width of the wider one.
        {"f{00..10}", "f05", true},
        {"f{00..10}", "f5", false},
        {"f{00..10}", "f010", false},
        {"f{01..9}", "f09", true},
        {"f{01..9}", "f9", false},
        {"f{9..01}", "f05", true},
        {"f{9..01}", "f5", false},
        {"f{1..09}", "f5", true},
        {"f{1..09}", "f05", false},
        {"f{001..9}", "f005", true},
        {"f{01..100}", "f005", true},
        {"f{01..100}", "f05", false},
        {"f{0100..01}", "f0050", true},
        {"f{0100..01}", "f50", false},
        {"f{05..5}", "f05", true},
        {"f{5..05}", "f5", true},
        {"f{5..05}", "f05", false},

        /// Where a number ends is not known in advance.
        {"f{0..10}{0..10}", "f1010", true},
        {"f{0..10}{0..10}", "f110", true},
        {"f{0..10}{0..10}", "f1", false},
        {"f{1..12}3", "f123", true},
        {"{1..2}{a,b}", "2b", true},

        /// Not a range, but an enum of one alternative.
        {"{1...2}", "1...2", true},
        {"{1..}", "1..", true},
        {"{..1}", "..1", true},
        {"{1..a}", "1..a", true},
    });
}

TEST(Glob, CharactersThatAreSpecialInRegexp)
{
    check({
        /// These two are not escaped on the way to the regexp.
        {"^a", "a", true},
        {"^a", "^a", false},
        {"a$", "a", true},
        {"a$b", "a$b", false},
        {"a$b", "ab", false},
        {"a^b", "a^b", false},
        {"{^a,b}c", "ac", true},
        {"*^a", "a", true},
        {"*^a", "xa", false},

        /// The others are.
        {"a+b", "a+b", true},
        {"a+b", "aab", false},
        {"[ab]", "[ab]", true},
        {"[ab]", "a", false},
        {"(a|b)", "(a|b)", true},
        {"(a|b)", "a", false},
        {"a\\b", "a\\b", true},
        {"a.b", "a.b", true},
        {"a.b", "axb", false},
    });
}

TEST(Glob, Characters)
{
    check({
        {"?", "é", true},
        {"?", "€", true},
        {"?", "😀", true},
        {"??", "é", false},
        {"*é*", "xéy", true},

        /// A byte that is not a part of a character is matched by no wildcard.
        {"?", "\xFF", false},
        {"*", "a\xFF" "b", false},
        {"**", "a\xFF" "b", false},
        {"?", "\x80", false},
        {"?", "\xC3", false},
        {"?", "\xC0\x80", false},
        {"?", "\xF5\x80\x80\x80", false},
        /// `re2` takes these for characters.
        {"?", "\xE0\x80\x80", true},
        {"?", "\xED\xA0\x80", true},
        {"?", "\xF4\x90\x80\x80", true},
    });
}

TEST(Glob, PatternsWithoutRegexp)
{
    /// A range the regexp is refused for.
    EXPECT_THROW(makeRegexpPatternFromGlobs("f{0..1000000}"), Exception);
    {
        Glob glob("f{0..1000000}");
        EXPECT_TRUE(glob.matches("f0"));
        EXPECT_TRUE(glob.matches("f999999"));
        EXPECT_TRUE(glob.matches("f1000000"));
        EXPECT_FALSE(glob.matches("f1000001"));
        EXPECT_FALSE(glob.matches("f0999999"));
    }
    {
        Glob glob("{0..18446744073709551615}");
        EXPECT_TRUE(glob.matches("18446744073709551615"));
        EXPECT_FALSE(glob.matches("18446744073709551616"));
        EXPECT_FALSE(glob.matches("184467440737095516150"));
    }
    {
        Glob glob("{000000000000000000000001..300000}");
        EXPECT_TRUE(glob.matches("000000000000000000000002"));
        EXPECT_TRUE(glob.matches("000000000000000000300000"));
        EXPECT_FALSE(glob.matches("000000000000000000300001"));
        EXPECT_FALSE(glob.matches("100000000000000000000002"));
        EXPECT_FALSE(glob.matches("2"));
    }

    /// A bound that does not fit into 64 bits.
    EXPECT_THROW(Glob("{0..18446744073709551616}"), Exception);
    EXPECT_THROW(Glob("{99999999999999999999..1}"), Exception);

    /// A pattern that is not valid UTF-8.
    EXPECT_FALSE(RegexpOracle("a\xFF" "b*").exists());
    {
        Glob glob("a\xFF" "b*");
        EXPECT_TRUE(glob.matches("a\xFF" "bc"));
        EXPECT_FALSE(glob.matches("a\xFE" "bc"));
    }
}

TEST(Glob, Limits)
{
    /// Every wildcard is an instruction, and the end of the pattern is one more.
    EXPECT_NO_THROW(Glob(std::string(Glob::MAX_PROGRAM_SIZE - 1, '?')));
    EXPECT_THROW(Glob(std::string(Glob::MAX_PROGRAM_SIZE, '?')), Exception);

    /// A literal is one instruction whatever its length.
    {
        const std::string name(1000000, 'a');
        Glob glob(name + "*");
        EXPECT_TRUE(glob.matches(name + "b"));
        EXPECT_FALSE(glob.matches(name.substr(1)));
    }

    /// And so is an enum of literal alternatives, whatever their number.
    {
        std::string pattern = "{";
        for (size_t i = 0; i < 100000; ++i)
            pattern += "key" + std::to_string(i) + ",";
        pattern += "last}/*.csv";

        Glob glob(pattern);
        EXPECT_TRUE(glob.matches("key0/a.csv"));
        EXPECT_TRUE(glob.matches("key54321/a.csv"));
        EXPECT_TRUE(glob.matches("key99999/a.csv"));
        EXPECT_TRUE(glob.matches("last/a.csv"));
        EXPECT_FALSE(glob.matches("key100000/a.csv"));
        EXPECT_FALSE(glob.matches("key/a.csv"));
    }
}

TEST(Glob, NoExponentialBacktracking)
{
    std::string pattern;
    for (size_t i = 0; i < 30; ++i)
        pattern += "*a";
    pattern += "b";

    const std::string candidate(2000, 'a');

    Glob glob(pattern);
    EXPECT_FALSE(glob.matches(candidate));
    EXPECT_TRUE(glob.matches(candidate + "b"));
}

TEST(Glob, CopyAndMove)
{
    /// Short enough to be stored inside of the string object, which a copy and a move relocate.
    Glob original("{a,bc}{1..3}*");

    Glob copy = original;
    Glob moved = std::move(original);

    for (const Glob * glob : {&copy, &moved})
    {
        EXPECT_EQ(glob->getPattern(), "{a,bc}{1..3}*");
        EXPECT_TRUE(glob->matches("bc2x"));
        EXPECT_FALSE(glob->matches("b2x"));
    }
}

namespace
{

/// Makes random patterns and candidates for `SameAsRegexp`. What they are made of is chosen to reach
/// the corners of the language: characters the regexp is special about, bodies that are almost a
/// group, numbers written with zeros in front, characters of several bytes and bytes that are none.
class PatternGenerator
{
public:
    explicit PatternGenerator(UInt64 seed) : rng(seed) {} /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic seed for a reproducible test

    std::string pattern()
    {
        std::string res;
        const size_t num_parts = 1 + random(5);
        for (size_t i = 0; i < num_parts; ++i)
        {
            switch (random(8))
            {
                case 0:
                    res += "{" + pick(NUMBERS) + ".." + pick(NUMBERS) + "}";
                    break;
                case 1:
                {
                    res += '{';
                    const size_t num_alternatives = 1 + random(4);
                    for (size_t j = 0; j < num_alternatives; ++j)
                    {
                        if (j)
                            res += ',';
                        res += text(3, random(2));
                    }
                    res += '}';
                    break;
                }
                case 2:
                    res += pick(ALMOST_GROUPS);
                    break;
                case 3:
                    res += pick(SPECIAL);
                    break;
                case 4:
                    res += text(2, false);
                    break;
                default:
                    res += text(3, true);
            }
        }
        return res;
    }

    /// A string that has a good chance to match the pattern. It is put together without knowing what
    /// a group is, so it is the regexp that tells whether it does.
    std::string candidate(const std::string & pattern, size_t depth = 0)
    {
        std::string res;
        for (size_t i = 0; i < pattern.size();)
        {
            const char c = pattern[i];
            const size_t close = c == '{' && depth < 3 ? pattern.find_first_of("{}", i + 1) : std::string::npos;

            if (c == '*')
            {
                const size_t length = random(3);
                for (size_t j = 0; j < length; ++j)
                    res += random(4) ? pick(LITERAL) : pick(SEPARATORS);
                ++i;
            }
            else if (c == '?')
            {
                res += pick(LITERAL);
                ++i;
            }
            else if ((c == '^' || c == '$') && random(2))
            {
                ++i;
            }
            else if (close != std::string::npos && pattern[close] == '}')
            {
                const std::string body = pattern.substr(i + 1, close - i - 1);
                const size_t dots = body.find("..");

                if (random(6) == 0)
                {
                    res += '{' + body + '}';
                }
                else if (dots != std::string::npos && dots > 0 && random(4))
                {
                    std::string number = std::to_string(random(120));
                    const size_t width = random(2) ? dots : body.size() - dots - 2;
                    if (random(2))
                        number.insert(0, width > number.size() ? width - number.size() : 0, '0');
                    res += number;
                }
                else
                {
                    std::vector<std::string> alternatives;
                    size_t begin = 0;
                    while (true)
                    {
                        const size_t comma = body.find(',', begin);
                        alternatives.push_back(body.substr(begin, comma == std::string::npos ? comma : comma - begin));
                        if (comma == std::string::npos)
                            break;
                        begin = comma + 1;
                    }
                    res += candidate(alternatives[random(alternatives.size())], depth + 1);
                }
                i = close + 1;
            }
            else
            {
                res += c;
                ++i;
            }
        }

        /// Spoil it a little from time to time.
        if (random(4) == 0)
        {
            const size_t pos = random(res.size() + 1);
            if (random(2) && pos < res.size())
                res.erase(pos, 1);
            else
                res.insert(pos, random(6) ? pick(LITERAL) : pick(NOT_CHARACTERS));
        }

        return res;
    }

    std::string text(size_t max_length, bool with_special)
    {
        std::string res;
        const size_t length = random(max_length + 1);
        for (size_t i = 0; i < length; ++i)
        {
            const size_t kind = random(40);
            if (with_special && kind < 12)
                res += pick(SPECIAL);
            else if (kind == 39)
                res += pick(NOT_CHARACTERS);
            else
                res += pick(LITERAL);
        }
        return res;
    }

    size_t random(size_t limit) { return rng() % limit; }

private:
    using Pieces = std::vector<std::string>;

    const std::string & pick(const Pieces & pieces) { return pieces[random(pieces.size())]; }

    static inline const Pieces LITERAL = {"a", "b", "0", "1", "9", "/", "/", ".", "-", "_", "x", "é", "€", "😀"};
    static inline const Pieces SEPARATORS = {"/", "/", "{", "}"};
    static inline const Pieces SPECIAL = {"*", "*", "?", "**", "**/", "/**/", "***", "{", "}", ",", "..", "^", "$",
                            "\\", "[", "]", "(", ")", "|", "+", "{{", "}}"};
    static inline const Pieces NOT_CHARACTERS = {"\xFF", "\x80", "\xC3", "\xE2\x82", "\xC0\x80", "\xF5\x80\x80\x80",
                                   /// And the ones `re2` takes for characters.
                                   "\xED\xA0\x80", "\xF4\x90\x80\x80", "\xE0\x80\x80"};
    static inline const Pieces NUMBERS = {"0", "1", "2", "5", "7", "9", "10", "11", "19", "20", "99", "100", "101",
                            "00", "01", "05", "09", "007", "010", "0100", "000",
                            "18446744073709551615", "18446744073709551610", "018446744073709551615"};
    static inline const Pieces ALMOST_GROUPS = {"{a}", "{-}", "{\\}", "{[}", "{é}", "{}", "{,}", "{,a}", "{a,}", "{a,,b}",
                                  "{1..}", "{..1}", "{1...2}", "{1..2..3}", "{*}", "{?}", "{**/a,b}",
                                  "{a/**/b,c}", "{a,b}**/x", "{^a,b$}", "{.}", "{/}", "{ab", "ab}", "{+}", "{a-}"};

    std::mt19937_64 rng;
};

}

TEST(Glob, SameAsRegexp)
{
    PatternGenerator generator(1);

    size_t num_compared = 0;
    size_t num_matched = 0;
    size_t num_failures = 0;

    for (size_t i = 0; i < 20000 && num_failures < 20; ++i)
    {
        const std::string pattern = generator.pattern();

        RegexpOracle oracle(pattern);
        if (!oracle.exists())
            continue;

        const Glob glob(pattern);

        for (size_t j = 0; j < 20; ++j)
        {
            std::string candidate;
            switch (generator.random(6))
            {
                case 0:
                    candidate = generator.text(5, false);
                    break;
                case 1:
                    candidate = pattern;
                    break;
                default:
                    candidate = generator.candidate(pattern);
            }

            const bool expected = oracle.matches(candidate);
            const bool actual = glob.matches(candidate);

            ++num_compared;
            num_matched += expected;

            if (expected != actual)
            {
                ++num_failures;
                ADD_FAILURE() << "Pattern '" << pattern << "', regexp '" << makeRegexpPatternFromGlobs(pattern)
                    << "', candidate '" << candidate << "': the regexp says " << expected << ", and the glob says " << actual;
                break;
            }
        }
    }

    /// The test is worth something only while the candidates match often enough.
    EXPECT_GT(num_compared, size_t{300000});
    EXPECT_GT(num_matched * 5, num_compared);
}
