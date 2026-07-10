#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>

#include <Core/Field.h>
#include <Common/PODArray.h>

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace
{
using namespace DB;
using namespace std::literals::string_literals;
}

struct SplitByRegexpTokenizerTestCase
{
    const std::string_view description;
    const std::string regexp;
    const std::string source;
    const std::vector<std::string> tokens;
};

static std::ostream & operator<<(std::ostream & ostr, const SplitByRegexpTokenizerTestCase & test_case)
{
    return ostr << test_case.description;
}

class SplitByRegexpTokenizerTest : public ::testing::TestWithParam<SplitByRegexpTokenizerTestCase>
{
public:
    void SetUp() override
    {
        const auto & param = GetParam();
        const auto & source = param.source;
        data = std::make_unique<PaddedPODArray<char>>(source.data(), source.data() + source.size());

        /// Add predefined padding to ensure no reads past the end of the buffer: the substring-searcher
        /// fast path inside OptimizedRegularExpression may read a few bytes ahead.
        const char extra_padding[] = "this is the end \xd1\x8d\xd1\x82\xd0\xbe\xd0\xba\xd0\xbe \xd0\xbd\xd0\xb5\xd1\x86";
        data->insert(data->end(), std::begin(extra_padding), std::end(extra_padding));

        data->resize(data->size() - sizeof(extra_padding));
    }

    std::unique_ptr<PaddedPODArray<char>> data;
};

TEST_P(SplitByRegexpTokenizerTest, next)
{
    const auto & param = GetParam();

    SplitByRegexpTokenizer tokenizer(param.regexp);

    size_t i = 0;

    size_t pos = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    for (const auto & expected_token : param.tokens)
    {
        SCOPED_TRACE(++i);
        ASSERT_TRUE(tokenizer.nextInString(data->data(), data->size(), pos, token_start, token_len));

        EXPECT_EQ(expected_token, std::string_view(data->data() + token_start, token_len))
                << " token_start:" << token_start << " token_len: " << token_len;
    }
    ASSERT_FALSE(tokenizer.nextInString(data->data(), data->size(), pos, token_start, token_len))
            << "\n\t=> \"" << param.source.substr(token_start, token_len) << "\""
            << "\n\t" << token_start << ", " << token_len << ", " << pos << ", " << data->size();
}

/// Verify that the free-function driver `forEachToken` (used by index building) produces the same tokens.
TEST_P(SplitByRegexpTokenizerTest, forEachToken)
{
    const auto & param = GetParam();

    SplitByRegexpTokenizer tokenizer(param.regexp);

    std::vector<std::string> tokens;
    forEachToken(tokenizer, data->data(), data->size(),
        [&](const char * token_start, size_t token_len)
        {
            tokens.emplace_back(token_start, token_len);
            return false;
        });

    EXPECT_EQ(param.tokens, tokens);
}

INSTANTIATE_TEST_SUITE_P(Empty,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "Empty input produces no tokens.",
            ",",
            "",
            {}
        },
        {
            "Input consisting only of separators produces no tokens.",
            ",",
            ",,,",
            {}
        },
        {
            "A single separator produces no tokens.",
            ",",
            ",",
            {}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(SingleCharSeparator,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "Basic comma-separated values.",
            ",",
            "a,b,c",
            {"a", "b", "c"}
        },
        {
            "No separator present: the whole string is a single token.",
            ",",
            "abc",
            {"abc"}
        },
        {
            "Leading separator is ignored.",
            ",",
            ",a,b",
            {"a", "b"}
        },
        {
            "Trailing separator is ignored.",
            ",",
            "a,b,",
            {"a", "b"}
        },
        {
            "Consecutive separators do not produce empty tokens.",
            ",",
            "a,,b",
            {"a", "b"}
        },
        {
            "A letter as a separator splits around every occurrence.",
            "a",
            "banana",
            {"b", "n", "n"}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(RegexSemantics,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "Whitespace run as separator.",
            "\\s+",
            "a  b\tc\n\nd",
            {"a", "b", "c", "d"}
        },
        {
            "Character class as separator.",
            "[,;]",
            "a,b;c,d",
            {"a", "b", "c", "d"}
        },
        {
            "One-or-more quantifier on separator.",
            "-+",
            "a--b---c",
            {"a", "b", "c"}
        },
        {
            "Digit run as separator.",
            "\\d+",
            "a12b345c",
            {"a", "b", "c"}
        },
        {
            "Multi-character literal separator.",
            "::",
            "a::b::c",
            {"a", "b", "c"}
        },
        {
            "Alternation of literals as separator.",
            "and|or",
            "aandbborc",
            {"a", "bb", "c"}
        },
        {
            "Unescaped dot matches any character, so every position is a separator.",
            ".",
            "abc",
            {}
        },
        {
            "Escaped dot is a literal separator.",
            "\\.",
            "a.b.c",
            {"a", "b", "c"}
        },
        {
            "A separator that can only match empty (z*) never splits.",
            "z*",
            "abc",
            {"abc"}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(Anchors,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "The ^ anchor matches only at the true start of the buffer, not at each scan position.",
            "^a",
            "aba",
            {"ba"}
        },
        {
            "The $ anchor matches only at the true end of the buffer.",
            "a$",
            "banana",
            {"banan"}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(CaseSensitivity,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "Matching is case-sensitive: only the upper-case X splits.",
            "X",
            "aXbxc",
            {"a", "bxc"}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(UTF8,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "Multi-byte UTF-8 content is preserved inside tokens.",
            ",",
            "h\xC3\xA9llo,w\xC3\xB6rld",
            {"h\xC3\xA9llo", "w\xC3\xB6rld"}
        },
        {
            "A multi-byte UTF-8 character as a literal separator.",
            "\xE2\x86\x92", /// U+2192 RIGHTWARDS ARROW
            "a\xE2\x86\x92" "b\xE2\x86\x92" "c",
            {"a", "b", "c"}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(Longer,
    SplitByRegexpTokenizerTest,
    ::testing::ValuesIn(std::initializer_list<SplitByRegexpTokenizerTestCase>{
        {
            "A longer sentence split on non-word characters (\\W keeps '_' as a word character).",
            "\\W+",
            "The quick, brown fox! jumps-over the_lazy dog.",
            {"The", "quick", "brown", "fox", "jumps", "over", "the_lazy", "dog"}
        },
    })
);

TEST(SplitByRegexpTokenizerFactory, CreateAndDescribe)
{
    FieldVector args;
    args.push_back(Field(String(",")));

    auto tokenizer = TokenizerFactory::instance().get("splitByRegexp", args);
    ASSERT_NE(tokenizer, nullptr);
    EXPECT_EQ(tokenizer->getType(), ITokenizer::Type::SplitByRegexp);
    EXPECT_EQ(tokenizer->getDescription(), "splitByRegexp(',')");
    EXPECT_FALSE(tokenizer->supportsStringLike());
}

TEST(SplitByRegexpTokenizerFactory, MissingArgumentThrows)
{
    FieldVector args;
    EXPECT_ANY_THROW(TokenizerFactory::instance().get("splitByRegexp", args));
}

TEST(SplitByRegexpTokenizerFactory, EmptyRegexpThrows)
{
    FieldVector args;
    args.push_back(Field(String("")));
    EXPECT_ANY_THROW(TokenizerFactory::instance().get("splitByRegexp", args));
}

TEST(SplitByRegexpTokenizerFactory, TooManyArgumentsThrows)
{
    FieldVector args;
    args.push_back(Field(String(",")));
    args.push_back(Field(String(";")));
    EXPECT_ANY_THROW(TokenizerFactory::instance().get("splitByRegexp", args));
}
