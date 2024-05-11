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

// #include <pg_query.h>

#include <Common/logger_useful.h>
#include <Poco/Logger.h>

#include <string>

#include "config.h"

#include <base/scope_guard.h>
#include <iostream>

namespace DB
{

bool ParserPostgreSQLQuery::parseImpl(Pos & /*pos*/, ASTPtr & ast, Expected & /*expected*/)
{
    String json;

    json = PostgreSQL::Testing::SelectInt.PGAST;

    JSON::Element JSONRoot;
    JSON parser;

    if (!parser.parse(json, JSONRoot))
    {
        return false;
    }
    const auto root = PostgreSQL::buildJSONTree(JSONRoot);
    // PrintDebugInfoRecursive(root);
    ast = PostgreSQL::Transform(root);
    std::cerr << "Transform Finished\n";
    assert(ast);
    return true;
}
}
