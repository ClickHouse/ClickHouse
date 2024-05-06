#include "Parsers/Lexer.h"
#include <Parsers/PostgreSQL/ParserPostgreSQLQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>

#include <Poco/Logger.h>

#include <string>

#include <pg_query.h>

#include "config.h"

#include <PostgreSQL.h>

#include <base/scope_guard.h>

namespace DB
{

bool ParserPostgreSQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto * begin = pos->begin;
    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
        ++pos;
    const auto * end = pos->begin;
    
    const auto * begin_char_ptr = reinterpret_cast<char *>(begin);
    const auto * end_char_ptr = reinterpret_cast<char *>(end);
    String res = "";
    for (auto it = begin_char_ptr; it != end_char_ptr; it++) {
        res += *it;
    }
    
    Poco::Logger * log = &Poco::Logger::get("registerCompressionCodecWithType");

    LOG_DEBUG(log, "POSTGRESQL RES: {}", res);
    

    return true;
}
}
