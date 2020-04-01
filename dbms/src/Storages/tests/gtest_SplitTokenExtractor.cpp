#include <Storages/MergeTree/MergeTreeIndexFullText.h>

#include <Common/PODArray_fwd.h>
#include <Common/PODArray.h>

#include <gtest/gtest.h>

#include <vector>
#include <string_view>
#include <string>
#include <memory>

#include <immintrin.h>
#include <nmmintrin.h>
#include <emmintrin.h>

namespace
{
using namespace DB;
}

struct SplitTokenExtractorTestCase
{
    const char * description;
    const std::string source;
    const std::vector<std::string> tokens;
};

std::ostream & operator<<(std::ostream & ostr, const SplitTokenExtractorTestCase & test_case)
{
    return ostr << test_case.description;
}

class SplitTokenExtractorTest : public ::testing::TestWithParam<SplitTokenExtractorTestCase>
{
public:
    void SetUp() override
    {
        const auto & param = GetParam();
        const auto & source = param.source;
        data = std::make_unique<PaddedPODArray<const char>>(source.data(), source.data() + source.size());
    }

    std::unique_ptr<PaddedPODArray<const char>> data;
};

TEST_P(SplitTokenExtractorTest, next)
{
    const auto & param = GetParam();

    SplitTokenExtractor token_extractor;

    size_t i = 0;

    size_t pos = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    for (const auto & expected_token : param.tokens)
    {
        SCOPED_TRACE(++i);
        EXPECT_TRUE(token_extractor.next(data->data(), data->size(), &pos, &token_start, &token_len));
        EXPECT_EQ(expected_token, param.source.substr(token_start, token_len))
                << " token_start:" << token_start << " token_len: " << token_len;
    }
}

#define BINARY_STRING(str) std::string{str, sizeof(str)-1}

INSTANTIATE_TEST_SUITE_P(ShortSingleToken,
    SplitTokenExtractorTest,
    ::testing::ValuesIn(std::initializer_list<SplitTokenExtractorTestCase>{
        {
            "Empty input sequence produces no tokens.",
            "",
            {}
        },
        {
            "Short single token",
            "foo",
            {"foo"}
        },
        {
            "Short single token surruonded by whitespace",
            "\t\vfoo\n\r",
            {"foo"}
        }
    })
);

INSTANTIATE_TEST_SUITE_P(UTF8,
    SplitTokenExtractorTest,
    ::testing::ValuesIn(std::initializer_list<SplitTokenExtractorTestCase>{
        {
            "Single token with mixed ASCII and UTF-8 chars",
            BINARY_STRING("abc\u0442" "123\u0447XYZ\u043A"),
            {"abc\u0442" "123\u0447XYZ\u043A"}
        },
        {
            "Multiple UTF-8 tokens",
            BINARY_STRING("\u043F\u0440\u0438\u0432\u0435\u0442, u043C\u0438\u0440!"),
            {"\u043F\u0440\u0438\u0432\u0435\u0442", "u043C\u0438\u0440"}
        },
    })
);

INSTANTIATE_TEST_SUITE_P(MultipleTokens,
    SplitTokenExtractorTest,
    ::testing::ValuesIn(std::initializer_list<SplitTokenExtractorTestCase>{
        {
            "Multiple tokens separated by whitespace",
            BINARY_STRING("\nabc 123\tXYZ\r"),
            {
                "abc", "123", "XYZ"
            }
        },
        {
            "Multiple tokens separated by non-printable chars",
            BINARY_STRING("\0abc\1" "123\2XYZ\4"),
            {
                "abc", "123", "XYZ"
            }
        },
        {
            "ASCII table is split into numeric, upper case and lower case letters",

            BINARY_STRING("\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16"
            "\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNO"
            "PQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\x80\x81\x82\x83\x84\x85\x86\x87"
            "\x88\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95\x96\x97\x98\x99\x9a\x9b\x9c"
            "\x9d\x9e\x9f\xa0\xa1\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf\xb0\xb1"
            "\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6"
            "\xc7\xc8\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xdb"
            "\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0"
            "\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff"),
            {
                "0123456789", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
            }
        }
    })
);


INSTANTIATE_TEST_SUITE_P(SIMD_Cases,
    SplitTokenExtractorTest,
    ::testing::ValuesIn(std::initializer_list<SplitTokenExtractorTestCase>{
        {
            "First 16 bytes are empty, then a shor token",
            "                abcdef",
            {"abcdef"}
        },
        {
            "Token crosses bounday of 16-byte chunk",
            "            abcdef",
            {"abcdef"}
        },
        {
            "Token ends at the end of 16-byte chunk",
            "          abcdef",
            {"abcdef"}
        },
        {
            "Token crosses bondaries of multiple 16-byte chunks",
            "abcdefghijklmnopqrstuvwxyz",
            {"abcdefghijklmnopqrstuvwxyz"}
        },
    })
);
