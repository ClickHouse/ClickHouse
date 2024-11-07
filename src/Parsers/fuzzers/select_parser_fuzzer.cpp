#include <iostream>
#include <string>

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        std::string input = std::string(reinterpret_cast<const char*>(data), size);

        DB::ParserQueryWithOutput parser(input.data() + input.size());

        const UInt64 max_parser_depth = 1000;
        const UInt64 max_parser_backtracks = 1000000;
        DB::ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, max_parser_depth, max_parser_backtracks);

        const UInt64 max_ast_depth = 1000;
        ast->checkDepth(max_ast_depth);

        const UInt64 max_ast_elements = 50000;
        ast->checkSize(max_ast_elements);

        DB::WriteBufferFromOwnString wb;
        DB::formatAST(*ast, wb);

        std::cerr << wb.str() << std::endl;
    }
    catch (...)
    {
    }

    return 0;
}
