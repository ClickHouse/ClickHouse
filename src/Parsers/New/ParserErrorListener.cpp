#include <Common/Exception.h>
#include <common/logger_useful.h>

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

    LOG_ERROR(&Poco::Logger::get("ClickHouseParser"),
              "Last element parsed so far:\n"
              "{}\n"
              "Parser error: (pos {}) {}", parser->getRuleContext()->toStringTree(parser, true), token->getStartIndex(), message);

    throw DB::Exception("Can't parse input: " + message, ErrorCodes::SYNTAX_ERROR);
}

}
