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
#include <Parsers/StatementFactory.h>


namespace DB
{

bool ParserWatchQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_watch(Keyword::WATCH);
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p(true);
    ParserKeyword s_events(Keyword::EVENTS);
    ParserKeyword s_limit(Keyword::LIMIT);

    ASTPtr database;
    ASTPtr table;
    auto query = make_intrusive<ASTWatchQuery>();

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

namespace DB
{

REGISTER_STATEMENTS(Watch)
{
    factory.registerStatement("WATCH",
    {
        .description = R"(
Returns the successive results of a live view as they change. This statement is deprecated together with live views and
will be removed in the future.
)",
        .syntax = R"(
WATCH [db.]live_view [EVENTS] [LIMIT n] [FORMAT format]
)",
        .examples = {{"Watch a live view", "WATCH lv EVENTS LIMIT 1;", ""}},
        .related = {"CREATE VIEW", "SELECT"},
    });
}

}
