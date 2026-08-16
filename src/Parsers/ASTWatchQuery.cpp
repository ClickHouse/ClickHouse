#include <Parsers/ASTWatchQuery.h>

#include <Common/Exception.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTWatchQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "WatchQuery");
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("limit_length", limit_length);
    w.writeBool("is_watch_events", is_watch_events);
    writeOutputOptionsJSON(w);
}

void ASTWatchQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    database = r.readIdentifierChild("database");
    if (database)
        children.push_back(database);
    table = r.readIdentifierChild("table");
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing required 'table' in `WatchQuery` during AST JSON deserialization");
    children.push_back(table);

    limit_length = r.readChildOfType<ASTLiteral>("limit_length");
    if (limit_length)
    {
        const auto type = limit_length->as<ASTLiteral &>().value.getType();
        if (type != Field::Types::UInt64 && type != Field::Types::Int64 && type != Field::Types::Float64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "'limit_length' must be a numeric literal in `WatchQuery` during AST JSON deserialization");
    }
    is_watch_events = r.getBool("is_watch_events");
    readOutputOptionsJSON(r);
}

}
