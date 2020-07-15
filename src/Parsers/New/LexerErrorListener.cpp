#include <Parsers/New/LexerErrorListener.h>

#include <LexerNoViableAltException.h>


using namespace antlr4;

namespace DB
{

void LexerErrorListener::syntaxError(
    Recognizer * recognizer, Token *, size_t line, size_t pos, const std::string & message, std::exception_ptr e)
{
    std::cout << "Lexer error: " << message << std::endl;
}

}
