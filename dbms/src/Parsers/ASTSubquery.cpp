#include <Parsers/ASTSubquery.h>

namespace DB
{

String ASTSubquery::getColumnName() const
{
    /// This is a hack. We use alias, if available, because otherwise tree could change during analysis.
    return alias.empty() ? getTreeID() : alias;
}

}

