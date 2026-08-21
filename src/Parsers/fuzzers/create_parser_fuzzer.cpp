#include <iostream>
#include <string>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        std::string input = std::string(reinterpret_cast<const char*>(data), size);

        DB::ParserCreateQuery parser;

        const UInt64 max_parser_depth = 1000;
        const UInt64 max_parser_backtracks = 10000;
        DB::ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, max_parser_depth, max_parser_backtracks);

        const UInt64 max_ast_depth = 1000;
        ast->checkDepth(max_ast_depth);

        const UInt64 max_ast_elements = 50000;
        ast->checkSize(max_ast_elements);

        std::cerr << ast->formatWithSecretsOneLine() << std::endl;
    }
    catch (...)
    {
    }

    return 0;
}
