#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif


namespace DB
{


///CLUSTER PAUSE NODE SERVER:PORT, status is paused
///CLUSTER START NODE SERVER:PORT, status is activated
///TODO:
///CLUSTER ADD NODE SERVER:PORT SHARD REPLICA ON CLUSTER XXX, status is activated
///CLUSTER DROP NODE SERVER[:PORT], status is dropped
///CLUSTER REPLACE NODE OLD_SERVER[:OLD_PORT] NEW_SERVER[:NEW_PORT]
class ASTClusterQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum Type
    {
        UNKNOWN,
        ADD_NODE,
        PAUSE_NODE,
        DECOMMISSION_NODE,
        START_NODE,
        DROP_NODE,
        REPLACE_NODE,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    String server;
    UInt16 port;
    String new_server;
    UInt16 new_port;
    String shard;
    String replica;
    String database;

    String getID(char) const override { return "CLUSTER query"; }

    ASTPtr clone() const override { return std::make_shared<ASTClusterQuery>(*this); }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTClusterQuery>(clone(), new_database);
    }

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
