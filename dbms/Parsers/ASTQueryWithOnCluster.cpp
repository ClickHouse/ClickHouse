#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/queryToString.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>


namespace DB
{

std::string ASTQueryWithOnCluster::getRewrittenQueryWithoutOnCluster(const std::string & new_database) const
{
    return queryToString(getRewrittenASTWithoutOnCluster(new_database));
}


bool ASTQueryWithOnCluster::parse(Pos & pos, std::string & cluster_str, Expected & expected)
{
    if (!ParserKeyword{"CLUSTER"}.ignore(pos, expected))
        return false;

    return parseIdentifierOrStringLiteral(pos, expected, cluster_str);
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
