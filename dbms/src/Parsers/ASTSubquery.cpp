#include <Parsers/ASTSubquery.h>
#include <IO/WriteHelpers.h>

namespace DB
{

String ASTSubquery::getColumnNameImpl() const
{
    /// This is a hack. We use alias, if available, because otherwise tree could change during analysis.
    if (!alias.empty())
        return alias;

    Hash hash = getTreeHash();
    return "__subquery_" + toString(hash.first) + "_" + toString(hash.second);
}

}

