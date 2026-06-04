#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>
#include <Common/Exception.h>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    if (!cluster.empty())
        w.writeString("cluster", cluster);
}

void ASTKillQueryQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    if (!r.has("kill_type"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'kill_type' field in `KillQueryQuery` during AST JSON deserialization");
    Int64 type_value = r.getInt("kill_type");
    auto type_opt = magic_enum::enum_cast<Type>(static_cast<std::underlying_type_t<Type>>(type_value));
    if (!type_opt || static_cast<Int64>(*type_opt) != type_value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown KILL kill_type: {}", type_value);
    type = *type_opt;
    where_expression = r.readChild("where_expression");
    if (!where_expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing required 'where_expression' in `KillQueryQuery` during AST JSON deserialization");
    children.push_back(where_expression);
    sync = r.getBool("sync");
    test = r.getBool("test");
    cluster = r.getString("cluster");
}

}
