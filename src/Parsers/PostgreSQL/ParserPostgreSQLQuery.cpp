#include "Parsers/Lexer.h"
#include <Parsers/PostgreSQL/ParserPostgreSQLQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>

#include <Poco/Logger.h>

#include <string>

#include <pg_query.h>

#include "config.h"

// #include <PostgreSQL.h>

#include <base/scope_guard.h>

namespace DB
{

bool ParserPostgreSQLQuery::parseImpl(Pos & pos, [[maybe_unused]] ASTPtr & node, [[maybe_unused]] Expected & expected)
{
    const auto * begin = pos->begin;
    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
        ++pos;
    const auto * end = pos->begin;
    
    const auto * begin_char_ptr = begin;
    const auto * end_char_ptr = end;
    String res;
    for (const auto *it = begin_char_ptr; it != end_char_ptr; it++) {
        res += *it;
    }
    
    Poco::Logger * log = &Poco::Logger::get("registerCompressionCodecWithType");

    log->debug("POSTGRESQL RES: {}", res);
    

    return true;
}
}
