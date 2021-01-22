#include <Common/Exception.h>

#include <Parsers/New/LexerErrorListener.h>


using namespace antlr4;

namespace DB
{

namespace ErrorCodes
{

extern int SYNTAX_ERROR;

}

void LexerErrorListener::syntaxError(Recognizer *, Token *, size_t, size_t, const std::string & message, std::exception_ptr)
{
    std::cerr << "Lexer error: " << message << std::endl;

    throw DB::Exception("Can't recognize input: " + message, ErrorCodes::SYNTAX_ERROR);
}

}
