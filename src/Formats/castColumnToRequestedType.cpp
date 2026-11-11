#include <Formats/castColumnToRequestedType.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/castColumn.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/quoteString.h>

#include <boost/algorithm/string/predicate.hpp>

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

DataTypePtr alignStructFieldNamesCaseInsensitive(const DataTypePtr & from, const DataTypePtr & to)
{
    if (const auto * to_low = typeid_cast<const DataTypeLowCardinality *>(to.get()))
    {
        if (const auto * from_low = typeid_cast<const DataTypeLowCardinality *>(from.get()))
            return std::make_shared<DataTypeLowCardinality>(
                alignStructFieldNamesCaseInsensitive(from_low->getDictionaryType(), to_low->getDictionaryType()));
        return from;
    }
    if (const auto * to_null = typeid_cast<const DataTypeNullable *>(to.get()))
    {
        if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.get()))
            return std::make_shared<DataTypeNullable>(
                alignStructFieldNamesCaseInsensitive(from_null->getNestedType(), to_null->getNestedType()));
        return alignStructFieldNamesCaseInsensitive(from, to_null->getNestedType());
    }
    if (const auto * from_null = typeid_cast<const DataTypeNullable *>(from.get()))
        return std::make_shared<DataTypeNullable>(alignStructFieldNamesCaseInsensitive(from_null->getNestedType(), to));

    if (const auto * to_arr = typeid_cast<const DataTypeArray *>(to.get()))
    {
        if (const auto * from_arr = typeid_cast<const DataTypeArray *>(from.get()))
            return std::make_shared<DataTypeArray>(
                alignStructFieldNamesCaseInsensitive(from_arr->getNestedType(), to_arr->getNestedType()));
        return from;
    }
    if (const auto * to_map = typeid_cast<const DataTypeMap *>(to.get()))
    {
        if (const auto * from_map = typeid_cast<const DataTypeMap *>(from.get()))
            return std::make_shared<DataTypeMap>(
                alignStructFieldNamesCaseInsensitive(from_map->getKeyType(), to_map->getKeyType()),
                alignStructFieldNamesCaseInsensitive(from_map->getValueType(), to_map->getValueType()));
        return from;
    }
    if (const auto * to_tuple = typeid_cast<const DataTypeTuple *>(to.get()))
    {
        const auto * from_tuple = typeid_cast<const DataTypeTuple *>(from.get());
        if (!from_tuple)
            return from;

        /// Without names on both sides, elements are matched by position, so descend the same way: named structs
        /// nested inside still need their names aligned.
        if (!from_tuple->hasExplicitNames() || !to_tuple->hasExplicitNames())
        {
            const auto & from_elems = from_tuple->getElements();
            const auto & to_elems = to_tuple->getElements();
            if (from_elems.size() != to_elems.size())
                return from;
            DataTypes new_elems;
            new_elems.reserve(from_elems.size());
            for (size_t i = 0; i < from_elems.size(); ++i)
                new_elems.push_back(alignStructFieldNamesCaseInsensitive(from_elems[i], to_elems[i]));
            return from_tuple->hasExplicitNames()
                ? std::make_shared<DataTypeTuple>(new_elems, from_tuple->getElementNames())
                : std::make_shared<DataTypeTuple>(new_elems);
        }

        const auto & from_elems = from_tuple->getElements();
        const auto & from_names = from_tuple->getElementNames();
        const auto & to_elems = to_tuple->getElements();
        const auto & to_names = to_tuple->getElementNames();

        VectorWithMemoryTracking<UInt8> taken(to_names.size());
        for (const auto & name : from_names)
            if (auto pos = to_tuple->tryGetPositionByName(name))
                taken[*pos] = true;

        DataTypes new_elems;
        Strings new_names;
        new_elems.reserve(from_elems.size());
        new_names.reserve(from_names.size());
        for (size_t i = 0; i < from_elems.size(); ++i)
        {
            std::optional<size_t> pos = to_tuple->tryGetPositionByName(from_names[i]);
            if (!pos)
            {
                for (size_t j = 0; j < to_names.size() && !pos; ++j)
                    if (!taken[j] && boost::iequals(from_names[i], to_names[j]))
                        pos = j;
                if (pos)
                    taken[*pos] = true;
            }

            if (pos)
            {
                new_names.push_back(to_names[*pos]);
                new_elems.push_back(alignStructFieldNamesCaseInsensitive(from_elems[i], to_elems[*pos]));
            }
            else
            {
                new_names.push_back(from_names[i]);
                new_elems.push_back(from_elems[i]);
            }
        }
        return std::make_shared<DataTypeTuple>(new_elems, new_names);
    }
    return from;
}

}
