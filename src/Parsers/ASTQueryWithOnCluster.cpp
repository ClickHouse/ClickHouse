#include <Parsers/ASTQueryWithOnCluster.h>
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
    return getRewrittenASTWithoutOnCluster(params)->formatWithSecretsOneLine();
}


bool ASTQueryWithOnCluster::parse(Pos & pos, std::string & cluster_str, Expected & expected, bool * out_use_default_cluster)
{
    if (out_use_default_cluster)
        *out_use_default_cluster = false;

    if (!ParserKeyword(Keyword::CLUSTER).ignore(pos, expected))
        return false;

    if (parseIdentifierOrStringLiteral(pos, expected, cluster_str))
        return true;

    /// `ON CLUSTER` without an explicit cluster name: allowed only when the caller opts in.
    /// The name will be filled from the `default_cluster` setting during query execution.
    if (out_use_default_cluster)
    {
        *out_use_default_cluster = true;
        return true;
    }

    return false;
}


void ASTQueryWithOnCluster::formatOnCluster(WriteBuffer & ostr, const IAST::FormatSettings &) const
{
    if (!cluster.empty())
        ostr << " ON CLUSTER " << backQuoteIfNeed(cluster);
    else if (use_default_cluster)
        ostr << " ON CLUSTER";
}


}
