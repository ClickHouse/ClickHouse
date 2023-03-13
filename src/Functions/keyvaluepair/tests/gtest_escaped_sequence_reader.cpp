#include <Functions/keyvaluepair/src/impl/state/util/EscapedCharacterReader.h>
#include <gtest/gtest.h>

namespace DB
{

void test(std::string_view input, const std::vector<uint8_t> & expected_characters, std::size_t expected_bytes_read)
{
    auto read_result = EscapedCharacterReader::read(input);

    ASSERT_EQ(expected_characters, read_result.characters);

    auto bytes_read = read_result.ptr - input.begin();

    ASSERT_EQ(bytes_read, expected_bytes_read);
}

void test(std::string_view input, const std::vector<uint8_t> & expected_characters)
{
    return test(input, expected_characters, input.size());
}

TEST(EscapedCharacterReader, HexDigits)
{
    test("\\xA", {0xA});
    test("\\xAA", {0xAA});
    test("\\xff", {0xFF});
    test("\\x0", {0x0});
    test("\\x00", {0x0});

    test("\\xA$", {0xA}, 3);
    test("\\xAA$", {0xAA}, 4);
    test("\\xff$", {0xFF}, 4);
    test("\\x0$", {0x0}, 3);
    test("\\x00$", {0x0}, 4);

    test("\\x", {});
}

TEST(EscapedCharacterReader, OctalDigits)
{
    test("\\0377", {0xFF});
    test("\\0137", {0x5F});
    test("\\013", {0xB});
    test("\\0000", {0x0});
    test("\\000", {0x0});
    test("\\00", {0x0});

    test("\\0377$", {0xFF}, 5);
    test("\\0137$", {0x5F}, 5);
    test("\\013$", {0xB}, 4);
    test("\\0000$", {0x0}, 5);
    test("\\000$", {0x0}, 4);
    test("\\00$", {0x0}, 3);
}

TEST(EscapedCharacterReader, RegularEscapeSequences)
{
    test("\\n", {0xA}, 2);
    test("\\r", {0xD}, 2);
    test("\\r", {0xD}, 2);
}

TEST(EscapedCharacterReader, RandomEscapedCharacters)
{
    test("\\h", {'\\', 'h'});
    test("\\g", {'\\', 'g'});
    test("\\", {});
}

}
