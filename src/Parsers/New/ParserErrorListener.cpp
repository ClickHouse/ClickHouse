#include <Common/Exception.h>

#include <Parsers/New/ParserErrorListener.h>

#include <Parsers/New/ClickHouseParser.h>

#include <Exceptions.h>
#include <Token.h>


using namespace antlr4;

namespace DB
{

namespace ErrorCodes
{

extern int SYNTAX_ERROR;

}

void ParserErrorListener::syntaxError(
    Recognizer * recognizer, Token * token, size_t line, size_t pos, const std::string & message, std::exception_ptr e)
{
    auto * parser = dynamic_cast<ClickHouseParser*>(recognizer);
    auto * stream = parser->getTokenStream();

    // std::cerr << parser->getContext()->toStringTree(true) << std::endl;

    std::string tokens[9];
    try
    {
        tokens[0] = stream->get(token->getTokenIndex() - 4)->getText();
        tokens[1] = stream->get(token->getTokenIndex() - 3)->getText();
        tokens[2] = stream->get(token->getTokenIndex() - 2)->getText();
        tokens[3] = stream->get(token->getTokenIndex() - 1)->getText();
        tokens[4] = token->getText();
        tokens[5] = stream->get(token->getTokenIndex() + 1)->getText();
        tokens[6] = stream->get(token->getTokenIndex() + 2)->getText();
        tokens[7] = stream->get(token->getTokenIndex() + 3)->getText();
        tokens[8] = stream->get(token->getTokenIndex() + 4)->getText();
    }
    catch (antlr4::IndexOutOfBoundsException &)
    {
    }

    std::cerr << "Parser error: " << message << std::endl;
    std::cerr << tokens[0]
       << " " << tokens[1]
       << " " << tokens[2]
       << " " << tokens[3]
       << " " << tokens[4]
       << " " << tokens[5]
       << " " << tokens[6]
       << " " << tokens[7]
       << " " << tokens[8]
       << std::endl;

    throw DB::Exception("Can't parse input", ErrorCodes::SYNTAX_ERROR);
}

}
