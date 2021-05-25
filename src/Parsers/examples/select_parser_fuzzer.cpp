#include <iostream>
#include <string>

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    std::string input = std::string(reinterpret_cast<const char*>(data), size);

    DB::ParserQueryWithOutput parser(input.data() + input.size());
    DB::ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);

    DB::formatAST(*ast, std::cerr);

    return 0;
}
catch (...)
{
    return 1;
}
