#include <Parsers/ASTDDLQueryWithOnCluster.h>
#include <Parsers/queryToString.h>

namespace DB
{

std::string ASTDDLQueryWithOnCluster::getRewrittenQueryWithoutOnCluster(const std::string & new_database) const
{
    return queryToString(getRewrittenASTWithoutOnCluster(new_database));
}

}
