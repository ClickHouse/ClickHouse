#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTClusterQuery.h>
#include <Parsers/IAST.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


const char * ASTClusterQuery::typeToString(Type type)
{
    switch (type)
    {
        case Type::ADD_NODE:
            return "ADD NODE";
        case Type::PAUSE_NODE:
            return "PAUSE NODE";
        case Type::DECOMMISSION_NODE:
            return "DECOMMISSION NODE";
        case Type::START_NODE:
            return "START NODE";
        case Type::DROP_NODE:
            return "DROP NODE";
        case Type::REPLACE_NODE:
            return "REPLACE NODE";
        default:
            throw Exception("Unknown CLUSTER query command", ErrorCodes::LOGICAL_ERROR);
    }
}


void ASTClusterQuery::formatImpl(const FormatSettings & s, FormatState &, FormatStateStacked) const
{
    auto print_ip_port = [&] {
        if (!server.empty())
        {
            s.ostr << " ";
            s.ostr << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(server) << (s.hilite ? hilite_none : "");
            if (port != 0)
            {
                s.ostr << ":" << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(DB::toString(port))
                       << (s.hilite ? hilite_none : "");
            }
        }
    };

    auto print_new_node = [&] {
        if (!new_server.empty())
        {
            s.ostr << " ";
            s.ostr << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(new_server) << (s.hilite ? hilite_none : "");
            if (new_port != 0)
            {
                s.ostr << ":" << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(DB::toString(new_port))
                       << (s.hilite ? hilite_none : "");
            }
        }
    };

    auto print_shard_replica = [&] {
        s.ostr << " ";
        if (!shard.empty())
        {
            s.ostr << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(shard) << (s.hilite ? hilite_none : "") << " ";
            s.ostr << (s.hilite ? hilite_identifier : "") << backQuoteIfNeed(replica) << (s.hilite ? hilite_none : "");
        }
    };

    s.ostr << (s.hilite ? hilite_keyword : "") << "CLUSTER ";
    s.ostr << typeToString(type) << (s.hilite ? hilite_none : "");

    print_ip_port();

    if (type == Type::ADD_NODE)
    {
        print_shard_replica();
    }
    else if (type == Type::REPLACE_NODE)
    {
        print_new_node();
    }

    formatOnCluster(s);
}


}
