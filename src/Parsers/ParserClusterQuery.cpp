#include <IO/ReadBufferFromMemory.h>
#include <Parsers/ASTClusterQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserClusterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Common/parseAddress.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>


namespace DB
{
bool ParserClusterQuery::parseServerPort(Pos & pos, String & server, UInt16 & port, Expected & expected)
{
    String s;
    bool is_server = true;
    port = 0; //default is 0
    while (true)
    {
        ReadBufferFromMemory in(pos->begin, pos->size());
        readString(s, in);
        if (in.count() != pos->size())
        {
            expected.add(pos, "string literal");
            return false;
        }

        if (is_server)
        {
            if (pos->type == TokenType::Number || pos->type == TokenType::Dot || pos->type == TokenType::StringLiteral
                || pos->type == TokenType::BareWord)
            {
                server += s;
            }
            else
            {
                return false;
            }
        }
        else
        {
            if (pos->type == TokenType::Number)
            {
                port = DB::parse<UInt16>(s.c_str());
                ++pos;
                return true;
            }
            else
            {
                return false;
            }
        }
        ++pos;

        if (pos->type == TokenType::Colon)
        {
            is_server = false;
            ++pos;
        }
    }
}

bool ParserClusterQuery::parseServerPort(Pos & pos, std::shared_ptr<ASTClusterQuery> & query, Expected & expected)
{
    Poco::Logger * log = &(Poco::Logger::get("ParserClusterQuery"));
    if (!parseServerPort(pos, query->server, query->port, expected))
    {
        return false;
    }

    if (query->type == ASTClusterQuery::REPLACE_NODE)
    {
        if (!parseServerPort(pos, query->new_server, query->new_port, expected))
        {
            return false;
        }
    }
    String cluster_str;
    ParserKeyword s_on("ON");
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        query->cluster = cluster_str;
    }

    if (query->type == ASTClusterQuery::REPLACE_NODE)
    {
        LOG_DEBUG(
            log,
            "server1 {}:{}, server2 {}:{}, cluster {}, PosType {}",
            query->server,
            query->port,
            query->new_server,
            query->new_port,
            query->cluster,
            pos->type);
    }
    else
    {
        LOG_DEBUG(log, "server {}:{}, cluster {}, PosType {}", query->server, query->port, query->cluster, pos->type);
    }
    return true;
}


bool ParserClusterQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_cluster("CLUSTER");
    ParserKeyword s_add_node("ADD NODE");
    ParserKeyword s_pause_node("PAUSE NODE");
    ParserKeyword s_start_node("START NODE");
    ParserKeyword s_drop_node("DROP NODE");
    ParserKeyword s_replace_node("REPLACE NODE");

    log = &(Poco::Logger::get("ParserClusterQuery"));

    if (!s_cluster.ignore(pos, expected))
    {
        return false;
    }

    auto query = std::make_shared<ASTClusterQuery>();
    node = query;

    if (s_add_node.ignore(pos, expected))
    {
        query->type = ASTClusterQuery::ADD_NODE;
    }
    else if (s_pause_node.ignore(pos, expected))
    {
        query->type = ASTClusterQuery::PAUSE_NODE;
    }
    else if (s_start_node.ignore(pos, expected))
    {
        query->type = ASTClusterQuery::START_NODE;
    }
    else if (s_drop_node.ignore(pos, expected))
    {
        query->type = ASTClusterQuery::DROP_NODE;
    }
    else if (s_replace_node.ignore(pos, expected))
    {
        query->type = ASTClusterQuery::REPLACE_NODE;
    }
    else
        return false;

    return parseServerPort(pos, query, expected);
}

}
