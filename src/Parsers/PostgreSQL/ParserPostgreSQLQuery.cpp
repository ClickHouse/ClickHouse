#include "Parsers/Lexer.h"
#include "Transform.h"
#include <Parsers/PostgreSQL/test/TestData/Select.h>
#include <Parsers/PostgreSQL/ParserPostgreSQLQuery.h>
#include <Parsers/PostgreSQL/Common/Types.h>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/iostream_debug_helpers.h>

#include <pg_query.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>

#include <string>

#include "config.h"

#include <base/scope_guard.h>
#include <iostream>

namespace DB
{

bool ParserPostgreSQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserSetQuery set_p;

    if (set_p.parse(pos, node, expected))
        return true;

    const auto * begin = pos->begin;

    // The same parsers are used in the client and the server, so the parser have to detect the end of a single query in case of multiquery queries
    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
        ++pos;

    const auto * end = pos->begin;

    String json(begin, end);

    JSON::Element json_root;
    JSON parser;

    if (!parser.parse(json, json_root))
    {
        return false;
    }
    const auto root = PostgreSQL::buildJSONTree(json_root);
    PrintDebugInfoRecursive(root);
    node = PostgreSQL::Transform(root);
    std::cerr << "Transform Finished\n";
    assert(node);
    return true;
}
}
