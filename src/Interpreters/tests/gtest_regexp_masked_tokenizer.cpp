#include <Interpreters/BloomFilter.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/TokenizerFactory.h>
#include <Interpreters/TokenMasks.h>

#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/re2.h>

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

std::unique_ptr<ITokenizer> makeTokenizer(std::string_view definition)
{
    return TokenizerFactory::instance().get(definition);
}

const RegexpMaskedTokenizer & asMasked(const ITokenizer & tokenizer)
{
    return assert_cast<const RegexpMaskedTokenizer &>(tokenizer);
}

/// The spans of `input`; the input is copied into a buffer without padding.
std::vector<TokenSpan> getSpans(const RegexpMaskedTokenizer & tokenizer, std::string_view input)
{
    std::vector<char> buffer(input.begin(), input.end());
    std::vector<TokenSpan> spans;
    tokenizer.forEachSpan(buffer.data(), buffer.size(), [&](const TokenSpan & span)
    {
        spans.push_back(span);
        return false;
    });
    return spans;
}

/// Tokens of `input`, masked tokens written as `<token>`.
std::vector<String> getMarkedTokens(const RegexpMaskedTokenizer & tokenizer, std::string_view input)
{
    std::vector<String> result;
    for (const auto & span : getSpans(tokenizer, input))
    {
        String token(input.substr(span.offset, span.length));
        result.push_back(span.masked ? "<" + token + ">" : token);
    }
    return result;
}

/// Tokens of `input` with `forEachToken` on padded data, as the index build does.
std::vector<String> getTokens(const ITokenizer & tokenizer, std::string_view input)
{
    PaddedPODArray<char> buffer(input.data(), input.data() + input.size());
    std::vector<String> result;
    forEachToken(tokenizer, buffer.data(), buffer.size(), [&](const char * token, size_t length)
    {
        result.emplace_back(token, length);
        return false;
    });
    return result;
}

std::vector<String> getTokensWithNextInString(const ITokenizer & tokenizer, std::string_view input)
{
    std::vector<String> result;
    size_t pos = 0;
    size_t token_start = 0;
    size_t token_length = 0;
    while (pos < input.size() && tokenizer.nextInString(input.data(), input.size(), pos, token_start, token_length))
        result.emplace_back(input.substr(token_start, token_length));
    return result;
}

/// True if `[from, to)` is a sequence of whole separators of the base tokenizer.
bool consistsOfSeparators(const ITokenizer & base, std::string_view input, size_t from, size_t to)
{
    if (base.getType() == ITokenizer::Type::SplitByNonAlpha)
    {
        for (size_t i = from; i < to; ++i)
        {
            if (!SplitByNonAlphaTokenizer::separator_chars.contains(input[i]))
                return false;
        }
        return true;
    }

    const auto & split_by_string = assert_cast<const SplitByStringTokenizer &>(base);
    size_t pos = from;
    while (pos < to)
    {
        size_t separator_length = split_by_string.matchSeparator(input.data(), input.size(), pos);
        if (separator_length == 0)
            return false;
        pos += separator_length;
    }
    return pos == to;
}

String randomString(std::mt19937 & rng, size_t max_length)
{
    static const std::vector<std::string_view> pieces = {
        "a", "b", "f", "x", "Z", "e", "t", "0", "1", "9", "7", ".", ":", "::", "/", "\\", "-", "+", "_", "%", " ", ",", ", ",
        "=", "[", "]", "jan", "Nov", "0x", "http", "://", "2026-11-05", "10:15:30", "10.0.0.1", "deadbeef", "sess_",
        "\xD0\x96", "\xE5\x8F\xAB", "\xF0\x9F\x98\x89", "\x80", "\xFF", "\xC3", std::string_view("\0", 1), "aa", "ab"};

    std::uniform_int_distribution<size_t> length_distribution(0, max_length);
    std::uniform_int_distribution<size_t> piece_distribution(0, pieces.size() - 1);

    String result;
    const size_t num_pieces = length_distribution(rng);
    for (size_t i = 0; i < num_pieces; ++i)
        result += pieces[piece_distribution(rng)];
    return result;
}

const std::vector<String> & corpus()
{
    static const std::vector<String> strings = {
        "",
        "took 1.5GB from 10.0.0.1:9000",
        "user=alice session=sess_9f2c1a7b3d4e5f60 took 1.5GB",
        "Connection to 10.0.0.1:9000 timed out after 30000 ms",
        "user=alice took 1.5GB at 2026-11-05 10:15:30.123+00:00",
        "DB::StorageSharedMergeTree::applyMetadataUpdate",
        "thread-0 c-mint-zn-32-server-9tgljnw-0",
        "Request URI: /?endpoint=x&compress=false",
        "read /var/lib/clickhouse/store/abc.bin, 0x7f3a, deadbeef, 97a133f8-4d41-4e4f-b869-db7c48b6da4c",
        "GET http://host:8123/?q=1 HTTP/1.1",
        "delta -5 and +3.2e5 and 45%",
        "at fe80::1:2 and ::1",
        "127.0.0.1 - - [04/Nov/2026:10:15:30 +0000] \"GET /index.html HTTP/1.1\" 200 512",
        "Nov  5 10:15:30 host sshd[123]: Accepted",
        R"(C:\Users\x\file.txt and read/write TCP/IP 10/20 11/15/2026)",
        "\xD0\x9F\xD1\x80\xD0\xB8 10.0.0.1x 50%x \xE5\x8F\xAB",
    };
    return strings;
}

const std::vector<String> & tokenizerDefinitions()
{
    static const std::vector<String> definitions = {
        "regexpMasked",
        "regexpMasked(masks = [])",
        "regexpMasked(masks = ['ip', 'number'])",
        "regexpMasked(custom_masks = ['sess_[0-9a-f]{16}', 'x+:+'])",
        R"(regexpMasked(masks = [], custom_masks = ['[a-z]+\\.[a-z]+', '\\d+\\s+\\d+']))",
        "regexpMasked(splitByString)",
        "regexpMasked(splitByString([' ', ',']))",
        "regexpMasked(splitByString([', ', ' ', '::']), custom_masks = ['a,'])",
        "regexpMasked(splitByString(['aa', 'b']), masks = [], custom_masks = ['za', 'f.'])",
    };
    return definitions;
}

}

TEST(RegexpMaskedTokenizer, BuiltinMasks)
{
    auto tokenizer = makeTokenizer("regexpMasked");
    const auto & masked = asMasked(*tokenizer);

    auto check = [&](std::string_view input, std::vector<String> expected)
    {
        EXPECT_EQ(getMarkedTokens(masked, input), expected) << "input: " << input;
    };

    check("took 1.5GB from 10.0.0.1:9000", {"took", "<1.5GB>", "from", "<10.0.0.1>", "<9000>"});
    check("user=alice session=sess_9f2c1a7b3d4e5f60 took 1.5GB", {"user", "alice", "session", "sess", "<9f2c1a7b3d4e5f60>", "took", "<1.5GB>"});

    /// timestamp
    check("at 2026-11-05 10:15:30.123+00:00 ok", {"at", "<2026-11-05 10:15:30.123+00:00>", "ok"});
    check("2026-11-05T10:15:30Z", {"<2026-11-05T10:15:30Z>"});
    check("2026-11-05", {"<2026-11-05>"});
    check("[04/Nov/2026:10:15:30 +0000]", {"<04/Nov/2026:10:15:30 +0000>"});
    check("Nov 15 10:15:30 host", {"<Nov 15 10:15:30>", "host"});
    check("Nov  5 10:15:30", {"<Nov  5 10:15:30>"});
    check("Nov 15 host", {"Nov", "<15>", "host"});

    /// ip
    check("10.0.0.1:9000", {"<10.0.0.1>", "<9000>"});
    check("at fe80::1:2 and", {"at", "<fe80::1:2>", "and"});
    check("1:2:3:4:5:6:7:8", {"<1:2:3:4:5:6:7:8>"});
    check("DB::Add", {"DB", "Add"});
    check("::1", {"<1>"});
    check("::ffff:10.0.0.1", {"ffff", "<10.0.0.1>"});

    /// uuid
    check("id 97a133f8-4d41-4e4f-b869-db7c48b6da4c.", {"id", "<97a133f8-4d41-4e4f-b869-db7c48b6da4c>"});

    /// hex
    check("0x7f3a,", {"<0x7f3a>"});
    check("deadbeef", {"deadbeef"});
    check("deadbeef1", {"<deadbeef1>"});

    /// number
    check("x=-5", {"x", "<5>"});
    check("kill -9", {"kill", "<9>"});
    check("delta +3.2e5 and 45%", {"delta", "<3.2e5>", "and", "<45%>"});
    check("1.5ms", {"<1.5ms>"});

    /// path
    check("/var/lib/clickhouse/x.bin", {"<var/lib/clickhouse/x.bin>"});
    check("/var/log", {"var", "log"});
    check("read/write TCP/IP", {"read", "write", "TCP", "IP"});
    check("10/20", {"<10>", "<20>"});
    check("11/15/2026", {"<11/15/2026>"});
    check("C:\\Users\\x", {"<C:\\Users\\x>"});

    /// url
    check("GET http://host:8123/?q=1 HTTP/1.1", {"GET", "<http://host:8123/?q=1>", "HTTP", "<1.1>"});

    /// No mask.
    check("DB::StorageSharedMergeTree::applyMetadataUpdate", {"DB", "StorageSharedMergeTree", "applyMetadataUpdate"});
    check("Request URI: /?endpoint=x&compress=false", {"Request", "URI", "endpoint", "x", "compress", "false"});
}

TEST(RegexpMaskedTokenizer, Rules)
{
    auto check = [&](std::string_view definition, std::string_view input, std::vector<String> expected)
    {
        auto tokenizer = makeTokenizer(definition);
        EXPECT_EQ(getMarkedTokens(asMasked(*tokenizer), input), expected) << definition << " on " << input;
    };

    /// The longest match wins: `timestamp` over `number`, `uuid` over `hex`.
    check("regexpMasked", "2026-11-05", {"<2026-11-05>"});
    check("regexpMasked(masks = ['number'])", "2026-11-05", {"<2026>", "<11>", "<05>"});

    /// A match is extended to the next separator.
    check("regexpMasked", "10.0.0.1x", {"<10.0.0.1x>"});
    check("regexpMasked", "50%x 50%", {"<50%x>", "<50%>"});

    /// Masks are tried only at base-token starts, not inside a base token or at a separator.
    check("regexpMasked", "xyz123 -5", {"xyz123", "<5>"});
    check("regexpMasked(masks = [], custom_masks = ['-\\\\d'])", "a -5", {"a", "5"});

    /// Multi-byte separators.
    check("regexpMasked(splitByString([', ', ' ']))", "a, 10.0.0.1, b", {"a", "<10.0.0.1>", "b"});
    check("regexpMasked(splitByString([', ']), masks = [], custom_masks = ['foo,'])", "foo, bar, baz", {"<foo, bar>", "baz"});
    /// The match ends where a separator starts, so it is not extended; scanning resumes there.
    check("regexpMasked(splitByString(['aa']), masks = [], custom_masks = ['za'])", "zaaab", {"<za>", "b"});
    EXPECT_EQ(getTokens(*makeTokenizer("splitByString(['aa'])"), "zaaab"), (std::vector<String>{"z", "ab"}));

    /// Custom masks are case-insensitive.
    check("regexpMasked(masks = [], custom_masks = ['sess_[0-9a-f]{4}'])", "SESS_9F2C ok", {"<SESS_9F2C>", "ok"});
}

TEST(RegexpMaskedTokenizer, Properties)
{
    std::mt19937 rng(42); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic test data
    std::vector<String> inputs = corpus();
    for (size_t i = 0; i < 3000; ++i)
        inputs.push_back(randomString(rng, 30));

    for (const auto & definition : tokenizerDefinitions())
    {
        auto tokenizer = makeTokenizer(definition);
        const auto & masked = asMasked(*tokenizer);
        const auto & base = masked.getBase();
        const auto & masks = masked.getMasks();
        const bool no_masks = masks.getBuiltinNames().empty() && masks.getCustomPatterns().empty();

        for (const auto & input : inputs)
        {
            SCOPED_TRACE(definition + " on '" + input + "'");
            const auto spans = getSpans(masked, input);

            std::vector<String> tokens;
            for (const auto & span : spans)
                tokens.emplace_back(input.substr(span.offset, span.length));

            /// The ways to get the tokens agree.
            EXPECT_EQ(getTokens(masked, input), tokens);
            EXPECT_EQ(getTokensWithNextInString(masked, input), tokens);
            VectorWithMemoryTracking<String> string_tokens;
            tokenizer->stringToTokens(input.data(), input.size(), string_tokens);
            EXPECT_EQ(std::vector<String>(string_tokens.begin(), string_tokens.end()), tokens);

            /// Without masks, the tokens are the base tokens.
            if (no_masks)
                EXPECT_EQ(tokens, getTokens(base, input));

            /// Spans are ordered, non-empty and separated by whole separators.
            size_t previous_end = 0;
            for (size_t i = 0; i < spans.size(); ++i)
            {
                const auto & span = spans[i];
                ASSERT_GT(span.length, 0u);
                ASSERT_LE(span.offset + span.length, input.size());
                ASSERT_GE(span.offset, previous_end);
                if (i > 0)
                    EXPECT_GT(span.offset, previous_end) << "adjacent tokens";
                EXPECT_TRUE(consistsOfSeparators(base, input, previous_end, span.offset)) << "gap before token " << i;
                previous_end = span.offset + span.length;
            }
            EXPECT_TRUE(consistsOfSeparators(base, input, previous_end, input.size())) << "trailing gap";

            /// Cutting at a token boundary does not change the tokens.
            for (const auto & span : spans)
            {
                for (size_t cut : {span.offset, span.offset + span.length})
                {
                    std::vector<TokenSpan> pieces = getSpans(masked, std::string_view(input).substr(0, cut));
                    for (auto right : getSpans(masked, std::string_view(input).substr(cut)))
                    {
                        right.offset += cut;
                        pieces.push_back(right);
                    }

                    ASSERT_EQ(pieces.size(), spans.size()) << "cut at " << cut;
                    for (size_t i = 0; i < spans.size(); ++i)
                    {
                        EXPECT_EQ(pieces[i].offset, spans[i].offset) << "cut at " << cut;
                        EXPECT_EQ(pieces[i].length, spans[i].length) << "cut at " << cut;
                        EXPECT_EQ(pieces[i].masked, spans[i].masked) << "cut at " << cut;
                    }
                }
            }

            /// A position rejected by the first-byte table has no match.
            for (size_t pos = 0; pos < input.size(); ++pos)
            {
                TokenMasks::ScanState scan_state;
                if (!masks.mayStartWith(input[pos]))
                    EXPECT_EQ(masks.matchAt(input.data() + pos, input.size() - pos, scan_state), 0u) << "at " << pos;
            }
        }
    }
}

TEST(RegexpMaskedTokenizer, CaseInsensitive)
{
    std::mt19937 rng(7); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic test data
    std::vector<String> inputs = corpus();
    for (size_t i = 0; i < 3000; ++i)
        inputs.push_back(randomString(rng, 30));

    auto to_case = [](String s, bool upper)
    {
        for (auto & c : s)
            c = upper ? toUpperASCII(c) : toLowerASCII(c);
        return s;
    };

    for (std::string_view definition : {"regexpMasked", "regexpMasked(splitByString([' ', ', ', '::']), custom_masks = ['sess_[0-9a-f]+'])"})
    {
        auto tokenizer = makeTokenizer(definition);
        for (const auto & input : inputs)
        {
            const auto tokens = getTokens(*tokenizer, input);
            for (bool upper : {false, true})
            {
                std::vector<String> expected;
                for (const auto & token : tokens)
                    expected.push_back(to_case(token, upper));
                EXPECT_EQ(getTokens(*tokenizer, to_case(input, upper)), expected) << definition << " on '" << input << "'";
            }
        }
    }
}

TEST(RegexpMaskedTokenizer, EdgeCases)
{
    auto tokenizer = makeTokenizer("regexpMasked");
    const auto & masked = asMasked(*tokenizer);

    EXPECT_TRUE(getSpans(masked, "").empty());
    EXPECT_TRUE(getSpans(masked, " ,;: ").empty());

    /// Invalid UTF-8 is not a separator of `splitByNonAlpha`.
    EXPECT_EQ(getMarkedTokens(masked, "\xFF\xFE 10.0.0.1 \x80"), (std::vector<String>{"\xFF\xFE", "<10.0.0.1>", "\x80"}));

    const String long_word(1 << 20, 'a');
    EXPECT_EQ(getTokens(masked, long_word), std::vector<String>{long_word});

    const String long_url = "see http://" + long_word + " ok";
    EXPECT_EQ(getMarkedTokens(masked, long_url), (std::vector<String>{"see", "<http://" + long_word + ">", "ok"}));

    const String long_number(1 << 20, '7');
    EXPECT_EQ(getMarkedTokens(masked, long_number), std::vector<String>{"<" + long_number + ">"});
}

TEST(RegexpMaskedTokenizer, Validation)
{
    auto expect_rejected = [](std::string_view definition)
    {
        try
        {
            makeTokenizer(definition);
            ADD_FAILURE() << "accepted: " << definition;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << definition << ": " << e.message();
        }
    };

    expect_rejected("regexpMasked(masks = ['foo'])");
    expect_rejected("regexpMasked(masks = 'ip')");
    expect_rejected("regexpMasked(masks = [1])");
    expect_rejected("regexpMasked(masks = ['ip'], masks = ['uuid'])");
    expect_rejected("regexpMasked(foo = ['ip'])");
    expect_rejected("regexpMasked(masks = ['ip'], splitByNonAlpha)");
    expect_rejected("regexpMasked(ngrams)");
    expect_rejected("regexpMasked(splitByRegexp('a'))");
    expect_rejected("regexpMasked(regexpMasked)");
    expect_rejected("regexpMasked(custom_masks = ['('])");
    expect_rejected("regexpMasked(custom_masks = [''])");
    expect_rejected("regexpMasked(custom_masks = ['a*'])");
    expect_rejected("regexpMasked(custom_masks = ['a|'])");
    expect_rejected("regexpMasked(custom_masks = ['^abc'])");
    expect_rejected("regexpMasked(custom_masks = ['abc$'])");
    expect_rejected("regexpMasked(custom_masks = ['\\\\Aabc'])");
    expect_rejected("regexpMasked(custom_masks = ['abc\\\\z'])");
    expect_rejected("regexpMasked(custom_masks = ['\\\\babc'])");
    expect_rejected("regexpMasked(custom_masks = ['a\\\\Bb'])");
    expect_rejected("regexpMasked(custom_masks = ['(?m)a^b'])");
    expect_rejected("regexpMasked(custom_masks = ['" + String(TokenMasks::MAX_CUSTOM_MASK_SIZE + 1, 'a') + "'])");

    String many = "regexpMasked(custom_masks = [";
    for (size_t i = 0; i <= TokenMasks::MAX_CUSTOM_MASKS; ++i)
        many += fmt::format("{}'m{}'", i == 0 ? "" : ", ", i);
    expect_rejected(many + "])");

    /// The limits themselves are accepted.
    EXPECT_NO_THROW(makeTokenizer("regexpMasked(custom_masks = ['" + String(TokenMasks::MAX_CUSTOM_MASK_SIZE, 'a') + "'])"));
}

TEST(RegexpMaskedTokenizer, Description)
{
    const String defaults = "regexpMasked(splitByNonAlpha, masks = ['timestamp', 'ip', 'uuid', 'hex', 'number', 'path', 'url'])";

    for (std::string_view definition : {
             "regexpMasked",
             "regexpMasked()",
             "regexpMasked(splitByNonAlpha)",
             "regexpMasked('splitByNonAlpha')",
             "regexpMasked(custom_masks = [])",
             "regexpMasked(masks = ['url', 'path', 'number', 'hex', 'uuid', 'ip', 'timestamp', 'ip'])",
             "regexpMasked(splitByNonAlpha, masks = ['timestamp', 'ip', 'uuid', 'hex', 'number', 'path', 'url'])"})
        EXPECT_EQ(makeTokenizer(definition)->getDescription(), defaults) << definition;

    EXPECT_EQ(
        makeTokenizer("regexpMasked(custom_masks = ['b+', 'a+', 'b+'], masks = [])")->getDescription(),
        makeTokenizer("regexpMasked(masks = [], custom_masks = ['a+', 'b+'])")->getDescription());

    EXPECT_EQ(
        makeTokenizer("regexpMasked(splitByString, masks = [])")->getDescription(),
        "regexpMasked(splitByString([' ']), masks = [])");

    for (const auto & definition : tokenizerDefinitions())
    {
        const auto description = makeTokenizer(definition)->getDescription();
        EXPECT_EQ(makeTokenizer(description)->getDescription(), description) << definition;
    }

    const auto description = makeTokenizer(R"(regexpMasked(custom_masks = ['it\'s\\d+']))")->getDescription();
    EXPECT_EQ(description, "regexpMasked(splitByNonAlpha, masks = ['timestamp', 'ip', 'uuid', 'hex', 'number', 'path', 'url'], custom_masks = ['it\\'s\\\\d+'])");
    EXPECT_EQ(makeTokenizer(description)->getDescription(), description);
}

TEST(RegexpMaskedTokenizer, FirstBytes)
{
    auto first_bytes = [](std::string_view pattern, bool latin1 = false)
    {
        re2::RE2::Options options;
        options.set_case_sensitive(false);
        options.set_encoding(latin1 ? re2::RE2::Options::EncodingLatin1 : re2::RE2::Options::EncodingUTF8);
        re2::RE2 re(pattern, options);
        EXPECT_TRUE(re.ok()) << pattern;

        String result;
        auto set = TokenMasks::getFirstBytes(re);
        for (size_t byte = 0; byte < 256; ++byte)
        {
            if (set.contains(static_cast<char>(byte)))
                result += static_cast<char>(byte);
        }
        return result;
    };

    EXPECT_EQ(first_bytes("0x[0-9a-f]+"), "0");
    EXPECT_EQ(first_bytes("ab|cd", true), "ACac");
    EXPECT_EQ(first_bytes("1?2*(?:3|4)", true), "1234");
    EXPECT_EQ(first_bytes("\\d{0,2}x", true), "0123456789Xx");
    /// `k` folds with the Kelvin sign U+212A, `s` with U+017F, in UTF-8 mode.
    EXPECT_EQ(first_bytes("k"), "Kk\xE2");
    EXPECT_EQ(first_bytes("s"), "Ss\xC5");
    EXPECT_EQ(first_bytes("\xD0\x96"), "\xD0");
}

/// Each built-in mask gives exactly the longest match of its regular expressions, at every position, from any scan state
/// reached by scanning from the left: the prechecks, the hand-written matchers and the skipped starts are exact.
TEST(RegexpMaskedTokenizer, BuiltinMasksMatchTheirRegexps)
{
    re2::RE2::Options options;
    options.set_encoding(re2::RE2::Options::EncodingLatin1);
    options.set_case_sensitive(false);
    options.set_longest_match(true);

    std::vector<std::pair<TokenMasks::BuiltinPatternSpec, std::unique_ptr<re2::RE2>>> patterns;
    for (const auto & spec : TokenMasks::getBuiltinPatternSpecs())
    {
        patterns.emplace_back(spec, std::make_unique<re2::RE2>(spec.pattern, options));
        ASSERT_TRUE(patterns.back().second->ok()) << spec.pattern;
    }

    std::mt19937 rng(1); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): deterministic test data
    std::vector<String> inputs = corpus();
    for (size_t i = 0; i < 5000; ++i)
        inputs.push_back(randomString(rng, 40));

    for (const auto & mask : TokenMasks::getBuiltinMaskNames())
    {
        TokenMasks masks({mask}, {});
        for (const auto & input : inputs)
        {
            TokenMasks::ScanState scan_state;
            for (size_t pos = 0; pos < input.size(); ++pos)
            {
                const std::string_view text = std::string_view(input).substr(pos);
                size_t expected = 0;
                for (const auto & [spec, re] : patterns)
                {
                    std::string_view match;
                    if (spec.mask == mask && re->Match(text, 0, text.size(), re2::RE2::ANCHOR_START, &match, 1)
                        && (!spec.needs_digit || std::ranges::any_of(match, [](char c) { return isNumericASCII(c); })))
                        expected = std::max(expected, match.size());
                }

                EXPECT_EQ(masks.mayStartWith(text[0]) ? masks.matchAt(text.data(), text.size(), scan_state) : 0, expected)
                    << mask << " at " << pos << " of '" << input << "'";
            }
        }
    }
}

/// Inputs on which a scan that retried a mask at every start would be quadratic.
TEST(RegexpMaskedTokenizer, LinearTime)
{
    auto tokenizer = makeTokenizer("regexpMasked");
    const auto & masked = asMasked(*tokenizer);

    const size_t n = 1 << 19;
    String repeated;
    for (size_t i = 0; i < n; ++i)
        repeated += "a.";

    for (const auto & [input, expected_count] : std::vector<std::pair<String, size_t>>{
             {repeated, n}, {repeated + "/b/", n + 1}, {"x/" + repeated, n + 1}, {repeated + ":/", n}})
    {
        size_t count = 0;
        masked.forEachSpan(input.data(), input.size(), [&](const TokenSpan &) { ++count; return false; });
        EXPECT_EQ(count, expected_count);

        /// The entry points used for needles (`hasAllTokens`, `hasPhrase`, index conditions), through the interface.
        const ITokenizer & tokenizer_interface = *tokenizer;
        VectorWithMemoryTracking<String> tokens;
        tokenizer_interface.stringToTokens(input.data(), input.size(), tokens);
        EXPECT_EQ(tokens.size(), expected_count);

        BloomFilter bloom_filter(BloomFilterParameters(1024, 3, 0));
        tokenizer_interface.stringToBloomFilter(input.data(), input.size(), bloom_filter);
        EXPECT_TRUE(bloom_filter.find("a", 1));
    }

    /// Separators of `splitByString` inside a run of hexadecimal digits without a decimal digit: every base token starts
    /// a `hex` candidate that spans the rest of the run and is rejected.
    String hex_run;
    for (size_t i = 0; i < n / 4; ++i)
        hex_run += "bbbbbbbba";

    for (std::string_view definition : {"regexpMasked(splitByString(['a']))", "regexpMasked(splitByString(['a']), masks = ['hex'])"})
    {
        auto hex_tokenizer = makeTokenizer(definition);
        size_t count = 0;
        asMasked(*hex_tokenizer).forEachSpan(hex_run.data(), hex_run.size(), [&](const TokenSpan & span)
        {
            EXPECT_FALSE(span.masked);
            ++count;
            return false;
        });
        EXPECT_EQ(count, n / 4) << definition;

        VectorWithMemoryTracking<String> tokens;
        hex_tokenizer->stringToTokens(hex_run.data(), hex_run.size(), tokens);
        EXPECT_EQ(tokens.size(), n / 4) << definition;
    }
}
