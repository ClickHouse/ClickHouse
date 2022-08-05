#include <string_view>

struct ParserTestCase
{
    const std::string_view input_text;
    const char * expected_ast = nullptr;
};
