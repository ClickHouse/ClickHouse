#include <Parsers/ASTSubquery.h>
#include <IO/WriteHelpers.h>

namespace DB
{

String ASTSubquery::getColumnNameImpl() const
{
    Hash hash = getTreeHash();
    return "__subquery_" + toString(hash.first) + "_" + toString(hash.second);
}

}

