#include <stdexcept>
#include <Parsers/New/ParserErrorListener.h>


using namespace antlr4;

namespace DB
{

void ParserErrorListener::syntaxError(
    Recognizer * recognizer, Token *, size_t line, size_t pos, const std::string & message, std::exception_ptr e)
{
    std::cout << "Parser error: " << message << std::endl;
}

}
