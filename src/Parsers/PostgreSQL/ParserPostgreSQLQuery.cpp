#include "Parsers/Lexer.h"
#include "Transform.h"
#include "examples.h"
#include <Parsers/PostgreSQL/ParserPostgreSQLQuery.h>
#include <Parsers/PostgreSQL/Common/Types.h>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseQuery.h>


#include <Common/logger_useful.h>
#include <Poco/Logger.h>

#include <string>

#include "config.h"

#include <base/scope_guard.h>
#include <iostream>

namespace DB
{

bool ParserPostgreSQLQuery::parseImpl(Pos & /*pos*/, ASTPtr & /*ast*/, Expected & /*expected*/)
{
    auto json = examples[0].PGAST;
    JSON::Element JSONRoot;
    JSON parser;

    if (!parser.parse(json, JSONRoot))
    {
        return false;
    }
    const auto root = PostgreSQL::buildJSONTree(JSONRoot);
    // PostgreSQL::Transform(root, ast);
    std::cerr << "___________________________";
    const auto keys = root->ListChildKeys();
    std::cerr << "ListChildKeys: ";
    for (auto key : keys) {
        std::cerr << key << ' ';
    }
    std::cerr << "\n";
    std::cerr << "___________________________";
    return true;
}
}
