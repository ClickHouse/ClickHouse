#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/queryToString.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

std::string ASTQueryWithOnCluster::getRewrittenQueryWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    return queryToString(getRewrittenASTWithoutOnCluster(params));
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
