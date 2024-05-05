#include <Parsers/PostgreSQL/ParserPostgreSQLQuery.h>
#include <string>

#include "Parsers/Lexer.h"
#include "config.h"

#if USE_PostgreSQL
#    include <PostgreSQL.h>
#endif

#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

bool ParserPostgreSQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
#if !USE_PostgreSQL
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED, "PostgreSQL is not available. PostgreSQL may be disabled. Use another dialect!");
#else
    return true;
#endif
}
}
