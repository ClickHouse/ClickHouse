#include <Common/Exception.h>

#include <Parsers/New/ParserErrorListener.h>

#include <Parsers/New/ClickHouseParser.h>

#include <Token.h>


using namespace antlr4;

namespace DB
{

namespace ErrorCodes
{

extern int SYNTAX_ERROR;

}

void ParserErrorListener::syntaxError(
    Recognizer * recognizer, Token * token, size_t, size_t, const std::string & message, std::exception_ptr)
{
    auto * parser = dynamic_cast<ClickHouseParser*>(recognizer);

    std::cerr << "Last element parsed so far:" << std::endl
              << parser->getRuleContext()->toStringTree(parser, true) << std::endl
              << "Parser error: (pos " << token->getStartIndex() << ") " << message << std::endl;

    throw DB::Exception("Can't parse input: " + message, ErrorCodes::SYNTAX_ERROR);
}

}
