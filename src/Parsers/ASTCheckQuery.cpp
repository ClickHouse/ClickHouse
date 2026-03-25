#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

void ASTCheckTableQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CheckTableQuery");
    w.writeChild("database", database);
    w.writeChild("table", table);
    w.writeChild("partition", partition);
    if (!part_name.empty())
        w.writeString("part_name", part_name);
}

void ASTCheckTableQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    database = r.readChild("database");
    if (database)
        children.push_back(database);
    table = r.readChild("table");
    if (table)
        children.push_back(table);
    partition = r.readChild("partition");
    if (partition)
        children.push_back(partition);
    part_name = r.getString("part_name");
}

void ASTCheckAllTablesQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CheckAllTablesQuery");
}

void ASTCheckAllTablesQuery::readJSON(const Poco::JSON::Object & /* json */)
{
    /// No fields to read.
}

}
