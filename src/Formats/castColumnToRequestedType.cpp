#include <Formats/castColumnToRequestedType.h>

#include <Interpreters/castColumn.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{

void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type)
{
    if (column.type->equals(*requested_type))
        return;

    try
    {
        column.column = castColumn(column, requested_type);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
            "while converting column {} from type {} to type {}",
            backQuote(column.name), column.type->getName(), requested_type->getName()));
        throw;
    }

    column.type = requested_type;
}

}
