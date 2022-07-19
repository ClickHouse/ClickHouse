/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

bool ParserWatchQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_watch("WATCH");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p(true);
    ParserKeyword s_events("EVENTS");
    ParserKeyword s_limit("LIMIT");

    ASTPtr database;
    ASTPtr table;
    auto query = std::make_shared<ASTWatchQuery>();

    if (!s_watch.ignore(pos, expected))
    {
        return false;
    }

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    /// EVENTS
    if (s_events.ignore(pos, expected))
    {
        query->is_watch_events = true;
    }

    /// LIMIT length
    if (s_limit.ignore(pos, expected))
    {
        ParserNumber num;

        if (!num.parse(pos, query->limit_length, expected))
            return false;
    }

    query->database = database;
    query->table = table;

    if (database)
        query->children.push_back(database);

    if (table)
        query->children.push_back(table);

    node = query;

    return true;
}


}
