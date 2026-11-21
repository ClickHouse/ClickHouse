#include <gtest/gtest.h>

#include "config.h"

#if USE_ICU

#include <Common/Base64.h>
#include <Interpreters/BPETokenizer.h>

#include <vector>

using namespace DB;

/// The expected values are produced by `tiktoken`: the pieces by the regular expressions of its `r50k_base`,
/// `cl100k_base` and `o200k_base` encodings, and the ids by an encoding with the vocabulary below and the
/// `cl100k_base` regular expression.

namespace
{

String repeat(const String & s, size_t times)
{
    String result;
    for (size_t i = 0; i < times; ++i)
        result += s;
    return result;
}

/// Every byte, then `tokens`, with the ranks in this order.
String vocabularyOf(const std::vector<String> & tokens)
{
    String contents;
    for (size_t i = 0; i < 256; ++i)
        contents += base64Encode(String(1, static_cast<char>(i))) + " " + std::to_string(i) + "\n";
    for (size_t i = 0; i < tokens.size(); ++i)
        contents += base64Encode(tokens[i]) + " " + std::to_string(256 + i) + "\n";
    return contents;
}

struct PieceCase
{
    BPEPretokenizer pretokenizer;
    String text;
    std::vector<size_t> lengths;
};

const std::vector<PieceCase> piece_cases = {
    {BPEPretokenizer::R50k, "Hello, world! It's a test.", {5, 1, 6, 1, 3, 2, 2, 5, 1}},
    {BPEPretokenizer::R50k, "I'LL do it, you'Ve been, they're, I'm, he'd", {
        1, 1, 2, 3, 3, 1, 4, 1, 2, 5, 1, 5, 3, 1, 2, 2, 1, 3, 2
    }},
    {BPEPretokenizer::R50k, "Numbers 1234567 and 3.14159, 0x1F", {7, 8, 4, 2, 1, 5, 1, 2, 1, 1, 1}},
    {BPEPretokenizer::R50k, "  leading and   multiple   spaces  ", {1, 8, 4, 2, 9, 2, 7, 2}},
    {BPEPretokenizer::R50k, "tabs\tand\nnew\r\nlines\n\n\nend\n", {4, 1, 3, 1, 3, 1, 1, 5, 2, 1, 3, 1}},
    {BPEPretokenizer::R50k, "trailing spaces   ", {8, 7, 3}},
    {BPEPretokenizer::R50k, "Привет, мир! Ünïcödé café naïve", {12, 1, 7, 1, 12, 6, 7}},
    {BPEPretokenizer::R50k, "中文字符和日本語のテキスト", {39}},
    {BPEPretokenizer::R50k, "emoji 🙂🚀 and symbols @#$%^&*()", {5, 9, 4, 8, 10}},
    {BPEPretokenizer::R50k, "camelCaseWords and HTTPServer XMLHttpRequest", {14, 4, 11, 15}},
    {BPEPretokenizer::R50k, "can't won't shouldn't", {3, 2, 4, 2, 8, 2}},
    {BPEPretokenizer::R50k, "a  b   c\n  d", {1, 1, 2, 2, 2, 2, 2}},
    {BPEPretokenizer::R50k, "ʰA! 'ſt z'ſ Ǆemo", {3, 1, 2, 3, 2, 1, 2, 6}},
    {BPEPretokenizer::Cl100k, "Hello, world! It's a test.", {5, 1, 6, 1, 3, 2, 2, 5, 1}},
    {BPEPretokenizer::Cl100k, "I'LL do it, you'Ve been, they're, I'm, he'd", {
        1, 3, 3, 3, 1, 4, 3, 5, 1, 5, 3, 1, 2, 2, 1, 3, 2
    }},
    {BPEPretokenizer::Cl100k, "Numbers 1234567 and 3.14159, 0x1F", {7, 1, 3, 3, 1, 4, 1, 1, 1, 3, 2, 1, 1, 1, 1, 1, 1}},
    {BPEPretokenizer::Cl100k, "  leading and   multiple   spaces  ", {1, 8, 4, 2, 9, 2, 7, 2}},
    {BPEPretokenizer::Cl100k, "tabs\tand\nnew\r\nlines\n\n\nend\n", {4, 4, 1, 3, 2, 5, 3, 3, 1}},
    {BPEPretokenizer::Cl100k, "trailing spaces   ", {8, 7, 3}},
    {BPEPretokenizer::Cl100k, "Привет, мир! Ünïcödé café naïve", {12, 1, 7, 1, 12, 6, 7}},
    {BPEPretokenizer::Cl100k, "中文字符和日本語のテキスト", {39}},
    {BPEPretokenizer::Cl100k, "emoji 🙂🚀 and symbols @#$%^&*()", {5, 9, 4, 8, 10}},
    {BPEPretokenizer::Cl100k, "camelCaseWords and HTTPServer XMLHttpRequest", {14, 4, 11, 15}},
    {BPEPretokenizer::Cl100k, "can't won't shouldn't", {3, 2, 4, 2, 8, 2}},
    {BPEPretokenizer::Cl100k, "a  b   c\n  d", {1, 1, 2, 2, 2, 1, 1, 2}},
    {BPEPretokenizer::Cl100k, "ʰA! 'ſt z'ſ Ǆemo", {3, 1, 2, 3, 2, 3, 6}},
    {BPEPretokenizer::O200k, "Hello, world! It's a test.", {5, 1, 6, 1, 5, 2, 5, 1}},
    {BPEPretokenizer::O200k, "I'LL do it, you'Ve been, they're, I'm, he'd", {4, 3, 3, 1, 7, 5, 1, 8, 1, 4, 1, 5}},
    {BPEPretokenizer::O200k, "Numbers 1234567 and 3.14159, 0x1F", {7, 1, 3, 3, 1, 4, 1, 1, 1, 3, 2, 1, 1, 1, 1, 1, 1}},
    {BPEPretokenizer::O200k, "  leading and   multiple   spaces  ", {1, 8, 4, 2, 9, 2, 7, 2}},
    {BPEPretokenizer::O200k, "tabs\tand\nnew\r\nlines\n\n\nend\n", {4, 4, 1, 3, 2, 5, 3, 3, 1}},
    {BPEPretokenizer::O200k, "trailing spaces   ", {8, 7, 3}},
    {BPEPretokenizer::O200k, "Привет, мир! Ünïcödé café naïve", {12, 1, 7, 1, 12, 6, 7}},
    {BPEPretokenizer::O200k, "中文字符和日本語のテキスト", {39}},
    {BPEPretokenizer::O200k, "emoji 🙂🚀 and symbols @#$%^&*()", {5, 9, 4, 8, 10}},
    {BPEPretokenizer::O200k, "camelCaseWords and HTTPServer XMLHttpRequest", {5, 4, 5, 4, 11, 8, 7}},
    {BPEPretokenizer::O200k, "can't won't shouldn't", {5, 6, 10}},
    {BPEPretokenizer::O200k, "a  b   c\n  d", {1, 1, 2, 2, 2, 1, 1, 2}},
    {BPEPretokenizer::O200k, "ʰA! 'ſt z'ſ Ǆemo", {2, 1, 1, 2, 3, 5, 6}},
};

const std::vector<String> merges = {
    "an", "na", "ba", "ana", "ban", "bana", "banana", " b", " banana", "aa", "aaaa", "aaaaaaaa", "ab", "к", "и", "в", "ки", "ви", "киви"};

const std::vector<std::pair<String, std::vector<UInt32>>> merge_cases = {
    {String("banana"), {262}},
    {String(" bananas"), {264, 115}},
    {repeat("a", 63), {267, 267, 267, 267, 267, 267, 267, 266, 265, 97}},
    {repeat("a", 64), {267, 267, 267, 267, 267, 267, 267, 267}},
    {repeat("a", 65), {267, 267, 267, 267, 267, 267, 267, 267, 97}},
    {repeat("a", 130), {267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 267, 265}},
    {repeat("ab", 40), {
        97, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258,
        258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258,
        98
    }},
    {repeat("banana", 11), {262, 262, 262, 262, 262, 262, 262, 262, 262, 262, 262}},
    {repeat("bananabananaxbanana", 4), {
        262, 262, 120, 262, 262, 262, 120, 262, 262, 262, 120, 262, 262, 262, 120, 262
    }},
    {String("киви киви киви "), {274, 32, 274, 32, 274, 32}},
    {repeat("ab", 32) + "a", {
        97, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258,
        258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258
    }},
};

}

TEST(BPETokenizer, Pretokenizers)
{
    for (const auto & [pretokenizer, text, expected] : piece_cases)
    {
        std::vector<size_t> lengths;
        for (size_t pos = 0; pos < text.size(); pos += lengths.back())
            lengths.push_back(nextBPEPiece(pretokenizer, text, pos));
        EXPECT_EQ(lengths, expected) << text;
    }
}

TEST(BPETokenizer, Merges)
{
    const auto vocabulary = BPEVocabulary::parse(vocabularyOf(merges), BPEPretokenizer::Cl100k);

    for (const auto & [text, expected] : merge_cases)
    {
        PaddedPODArray<UInt32> ids;
        vocabulary->encode(text, ids);
        EXPECT_EQ(std::vector<UInt32>(ids.begin(), ids.end()), expected) << text;
    }
}

TEST(BPETokenizer, InvalidVocabulary)
{
    const String valid = vocabularyOf({"ab"});
    EXPECT_NO_THROW(BPEVocabulary::parse(valid, BPEPretokenizer::Cl100k));
    EXPECT_ANY_THROW(BPEVocabulary::parse(valid.substr(valid.find('\n') + 1), BPEPretokenizer::Cl100k)); /// no token for byte 0
    EXPECT_ANY_THROW(BPEVocabulary::parse(valid + "YWI= 300\n", BPEPretokenizer::Cl100k)); /// the same token twice
    EXPECT_ANY_THROW(BPEVocabulary::parse(valid + "YWJj 256\n", BPEPretokenizer::Cl100k)); /// the same rank twice
    EXPECT_ANY_THROW(BPEVocabulary::parse(valid + "!!!! 300\n", BPEPretokenizer::Cl100k)); /// not base64
    EXPECT_ANY_THROW(BPEVocabulary::parse(valid + "YWJj 4000000000\n", BPEPretokenizer::Cl100k)); /// rank too large
}

#endif
