#include <string.h>

#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>

#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

namespace Nested
{

std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name)
{
    return nested_table_name + "." + nested_field_name;
}


/** Name can be treated as compound if and only if both parts are simple identifiers.
  */
std::pair<std::string, std::string> splitName(const std::string & name)
{
    const char * begin = name.data();
    const char * pos = begin;
    const char * end = begin + name.size();

    if (pos >= end || !isValidIdentifierBegin(*pos))
        return {name, {}};

    ++pos;

    while (pos < end && isWordCharASCII(*pos))
        ++pos;

    if (pos >= end || *pos != '.')
        return {name, {}};

    const char * first_end = pos;
    ++pos;
    const char * second_begin = pos;

    if (pos >= end || !isValidIdentifierBegin(*pos))
        return {name, {}};

    ++pos;

    while (pos < end && isWordCharASCII(*pos))
        ++pos;

    if (pos != end)
        return {name, {}};

    return {{ begin, first_end }, { second_begin, end }};
}


std::string extractTableName(const std::string & nested_name)
{
    auto splitted = splitName(nested_name);
    return splitted.first;
}


Block flatten(const Block & block)
{
    Block res;

    for (const auto & elem : block)
    {
        if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(elem.type.get()))
        {
            if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(type_arr->getNestedType().get()))
            {
                const DataTypes & element_types = type_tuple->getElements();
                const Strings & names = type_tuple->getElementNames();
                size_t tuple_size = element_types.size();

                bool is_const = elem.column->isColumnConst();
                const ColumnArray * column_array;
                if (is_const)
                    column_array = typeid_cast<const ColumnArray *>(&static_cast<const ColumnConst &>(*elem.column).getDataColumn());
                else
                    column_array = typeid_cast<const ColumnArray *>(elem.column.get());

                const ColumnPtr & column_offsets = column_array->getOffsetsPtr();

                const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(column_array->getData());
                const auto & element_columns = column_tuple.getColumns();

                for (size_t i = 0; i < tuple_size; ++i)
                {
                    String nested_name = concatenateName(elem.name, names[i]);
                    ColumnPtr column_array_of_element = ColumnArray::create(element_columns[i], column_offsets);

                    res.insert(ColumnWithTypeAndName(
                        is_const
                            ? ColumnConst::create(std::move(column_array_of_element), block.rows())
                            : std::move(column_array_of_element),
                        std::make_shared<DataTypeArray>(element_types[i]),
                        nested_name));
                }
            }
            else
                res.insert(elem);
        }
        else
            res.insert(elem);
    }

    return res;
}


NamesAndTypesList collect(const NamesAndTypesList & names_and_types)
{
    NamesAndTypesList res;

    std::map<std::string, NamesAndTypesList> nested;
    for (const auto & name_type : names_and_types)
    {
        bool collected = false;
        if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(name_type.type.get()))
        {
            auto splitted = splitName(name_type.name);
            if (!splitted.second.empty())
            {
                nested[splitted.first].emplace_back(splitted.second, type_arr->getNestedType());
                collected = true;
            }
        }

        if (!collected)
            res.push_back(name_type);
    }

    for (const auto & name_elems : nested)
        res.emplace_back(name_elems.first, std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(name_elems.second.getTypes(), name_elems.second.getNames())));

    return res;
}


void validateArraySizes(const Block & block)
{
    /// Nested prefix -> position of first column in block.
    std::map<std::string, size_t> nested;

    for (size_t i = 0, size = block.columns(); i < size; ++i)
    {
        const auto & elem = block.getByPosition(i);

        if (isArray(elem.type))
        {
            if (!typeid_cast<const ColumnArray *>(elem.column.get()))
                throw Exception("Column with Array type is not represented by ColumnArray column: " + elem.column->dumpStructure(), ErrorCodes::ILLEGAL_COLUMN);

            auto splitted = splitName(elem.name);

            /// Is it really a column of Nested data structure.
            if (!splitted.second.empty())
            {
                auto [it, inserted] = nested.emplace(splitted.first, i);

                /// It's not the first column of Nested data structure.
                if (!inserted)
                {
                    const ColumnArray & first_array_column = static_cast<const ColumnArray &>(*block.getByPosition(it->second).column);
                    const ColumnArray & another_array_column = static_cast<const ColumnArray &>(*elem.column);

                    if (!first_array_column.hasEqualOffsets(another_array_column))
                        throw Exception("Elements '" + block.getByPosition(it->second).name
                            + "' and '" + elem.name
                            + "' of Nested data structure '" + splitted.first
                            + "' (Array columns) have different array sizes.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
                }
            }
        }
    }
}

}

}
