#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>

#include <Columns/ColumnString.h>

#include <gtest/gtest.h>
#include <string_view>

namespace DB
{

void assert_byte_equality(StringRef lhs, const std::vector<uint8_t> & rhs)
{
    std::vector<uint8_t> lhs_vector {lhs.data, lhs.data + lhs.size};
    ASSERT_EQ(lhs_vector, rhs);
}

TEST(extractKVPairEscapingKeyValuePairExtractor, EscapeSequences)
{
    using namespace std::literals;

    auto extractor = KeyValuePairExtractorBuilder().buildWithEscaping();

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    auto pairs_count = extractor.extract(R"(key1:a\xFF key2:a\n\t\r)"sv, keys, values);

    ASSERT_EQ(pairs_count, 2u);
    ASSERT_EQ(keys->size(), pairs_count);
    ASSERT_EQ(keys->size(), values->size());

    ASSERT_EQ(keys->getDataAt(0).toView(), "key1");
    ASSERT_EQ(keys->getDataAt(1).toView(), "key2");

    assert_byte_equality(values->getDataAt(0), {'a', 0xFF});
    assert_byte_equality(values->getDataAt(1), {'a', 0xA, 0x9, 0xD});
}

}
