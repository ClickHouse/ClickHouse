#include <Parsers/queryToString.h>
#include <Storages/ColumnComment.h>

bool DB::operator== (const DB::ColumnComment& lhs, const DB::ColumnComment& rhs)
{
    return queryToString(lhs.expression) == queryToString(rhs.expression);
}
