#include <iostream>
#include <string>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        std::string input = std::string(reinterpret_cast<const char*>(data), size);

        DB::ParserCreateQuery parser;
        DB::ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 1000, DB::DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

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
