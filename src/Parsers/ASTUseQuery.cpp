#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

void ASTUseQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "UseQuery");
    w.writeChild("database", database);
}

void ASTUseQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    auto db_child = r.readChild("database");
    if (db_child)
        set(database, db_child);
}

}
