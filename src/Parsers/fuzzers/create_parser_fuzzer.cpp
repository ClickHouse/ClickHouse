#include <iostream>
#include <string>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    std::string input = std::string(reinterpret_cast<const char*>(data), size);

    DB::ParserCreateQuery parser;
    DB::ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 1000);

    DB::WriteBufferFromOwnString wb;
    DB::formatAST(*ast, wb);

    std::cerr << wb.str() << std::endl;

    return 0;
}
catch (...)
{
    return 1;
}
