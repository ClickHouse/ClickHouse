#include <Formats/castColumnToRequestedType.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
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
        column.column = castColumnMatchingTupleElementsByName(column, requested_type);
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

void matchTupleElementsByName(ColumnWithTypeAndName & column, const DataTypePtr & requested_type)
{
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(column.type).get());
    const auto * requested_tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(requested_type).get());
    if (!tuple_type || !requested_tuple_type || !tuple_type->hasExplicitNames() || !requested_tuple_type->hasExplicitNames())
        return;

    DataTypes element_types;
    element_types.reserve(requested_tuple_type->getElements().size());
    for (size_t i = 0; i < requested_tuple_type->getElements().size(); ++i)
    {
        auto pos = tuple_type->tryGetPositionByName(requested_tuple_type->getNameByPosition(i + 1));
        element_types.push_back(pos ? tuple_type->getElement(*pos) : requested_tuple_type->getElement(i));
    }

    DataTypePtr target_type = std::make_shared<DataTypeTuple>(element_types, requested_tuple_type->getElementNames());
    if (column.type->isNullable())
        target_type = std::make_shared<DataTypeNullable>(target_type);

    column.column = castColumnMatchingTupleElementsByName(column, target_type);
    column.type = target_type;
}

}
