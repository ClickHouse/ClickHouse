#include <Functions/keyvaluepair/src/KeyValuePairExtractorBuilder.h>
#include <gtest/gtest.h>
#include <string_view>

namespace DB
{

void assert_byte_equality(StringRef lhs, const std::vector<uint8_t> & rhs)
{
    std::vector<uint8_t> lhs_vector {lhs.data, lhs.data + lhs.size};
    ASSERT_EQ(lhs_vector, rhs);
}

TEST(EscapingKeyValuePairExtractor, EscapeSequences)
{
    using namespace std::literals;

    auto extractor = KeyValuePairExtractorBuilder().withEscaping().build();

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    auto pairs_count = extractor->extract(R"(key1:\xFF key2:\n\t\r key3:\0377)"sv, keys, values);

    ASSERT_EQ(pairs_count, 3u);
    ASSERT_EQ(keys->size(), pairs_count);
    ASSERT_EQ(keys->size(), values->size());

    ASSERT_EQ(keys->getDataAt(0), "key1");
    ASSERT_EQ(keys->getDataAt(1), "key2");
    ASSERT_EQ(keys->getDataAt(2), "key3");

    assert_byte_equality(values->getDataAt(0), {0xFF});
    assert_byte_equality(values->getDataAt(1), {0xA, 0x9, 0xD});
    assert_byte_equality(values->getDataAt(2), {0xFF});
}

}
