#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>

namespace DB
{

String ASTKillQueryQuery::getID(char delim) const
{
    return String("KillQueryQuery") + delim + (where_expression ? where_expression->getID() : "") + delim + String(sync ? "SYNC" : "ASYNC");
}

void ASTKillQueryQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "KILL ";

    switch (type)
    {
        case Type::Query:
            ostr << "QUERY";
            break;
        case Type::Mutation:
            ostr << "MUTATION";
            break;
        case Type::PartMoveToShard:
            ostr << "PART_MOVE_TO_SHARD";
            break;
        case Type::Transaction:
            ostr << "TRANSACTION";
            break;
    }

    formatOnCluster(ostr, settings);

    if (where_expression)
    {
        ostr << " WHERE ";
        where_expression->format(ostr, settings, state, frame);
    }

    ostr << " " << (test ? "TEST" : (sync ? "SYNC" : "ASYNC"));
}

void ASTKillQueryQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "KillQueryQuery");
    w.writeInt("kill_type", static_cast<Int64>(type));
    w.writeChild("where_expression", where_expression);
    if (sync)
        w.writeBool("sync", true);
    if (test)
        w.writeBool("test", true);
}

void ASTKillQueryQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    type = static_cast<Type>(r.getInt("kill_type"));
    where_expression = r.readChild("where_expression");
    if (where_expression)
        children.push_back(where_expression);
    sync = r.getBool("sync");
    test = r.getBool("test");
}

}
