#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/queryToString.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>


namespace DB
{

std::string ASTQueryWithOnCluster::getRewrittenQueryWithoutOnCluster(const std::string & new_database) const
{
    return queryToString(getRewrittenASTWithoutOnCluster(new_database));
}


bool ASTQueryWithOnCluster::parse(Pos & pos, Pos end, std::string & cluster_str, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;

    ws.ignore(pos, end);

    if (!ParserString{"CLUSTER", true, true}.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    Pos begin = pos;
    ASTPtr res;

    if (!ParserIdentifier().parse(pos, end, res, max_parsed_pos, expected))
    {
        pos = begin;
        if (!ParserStringLiteral().parse(pos, end, res, max_parsed_pos, expected))
            return false;
        else
            cluster_str = typeid_cast<const ASTLiteral &>(*res).value.safeGet<String>();
    }
    else
        cluster_str = typeid_cast<const ASTIdentifier &>(*res).name;

    return true;
}


void ASTQueryWithOnCluster::formatOnCluster(const IAST::FormatSettings & settings) const
{
    if (!cluster.empty())
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " ON CLUSTER " << (settings.hilite ? IAST::hilite_none : "")
        << backQuoteIfNeed(cluster);
    }
}


}
