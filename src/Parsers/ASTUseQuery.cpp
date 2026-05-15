#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTUseQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "UseQuery");
    w.writeChild("database", database);
}

void ASTUseQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    auto db_child = r.readChild("database");
    if (!db_child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing required 'database' in UseQuery JSON");
    set(database, db_child);
}

}
