#include <cstring>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>

#include <Parsers/IAST.h>

#include <boost/algorithm/string/case_conv.hpp>


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
    if (nested_table_name.empty())
        return nested_field_name;

    if (nested_field_name.empty())
        return nested_table_name;

    return nested_table_name + "." + nested_field_name;
}


/** Name can be treated as compound if it contains dot (.) in the middle.
  */
std::pair<std::string, std::string> splitName(const std::string & name, bool reverse)
{
    auto idx = (reverse ? name.find_last_of('.') : name.find_first_of('.'));
    if (idx == std::string::npos || idx == 0 || idx + 1 == name.size())
        return {name, {}};

    return {name.substr(0, idx), name.substr(idx + 1)};
}

std::pair<std::string_view, std::string_view> splitName(const std::string_view & name, bool reverse)
{
    auto idx = (reverse ? name.find_last_of('.') : name.find_first_of('.'));
    if (idx == std::string::npos || idx == 0 || idx + 1 == name.size())
        return {name, {}};

    return {name.substr(0, idx), name.substr(idx + 1)};
}


std::string extractTableName(const std::string & nested_name)
{
    auto split = splitName(nested_name);
    return split.first;
}


Block flatten(const Block & block)
{
    Block res;

    for (const auto & elem : block)
    {
        const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(elem.type.get());
        if (type_arr)
        {
            const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(type_arr->getNestedType().get());
            if (type_tuple && type_tuple->haveExplicitNames())
            {
                const DataTypes & element_types = type_tuple->getElements();
                const Strings & names = type_tuple->getElementNames();
                size_t tuple_size = element_types.size();

                bool is_const = isColumnConst(*elem.column);
                const ColumnArray * column_array;
                if (is_const)
                    column_array = typeid_cast<const ColumnArray *>(&assert_cast<const ColumnConst &>(*elem.column).getDataColumn());
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

namespace
{

using NameToDataType = std::map<String, DataTypePtr>;

NameToDataType getSubcolumnsOfNested(const NamesAndTypesList & names_and_types)
{
    std::unordered_map<String, NamesAndTypesList> nested;
    for (const auto & name_type : names_and_types)
    {
        const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(name_type.type.get());

        /// Ignore true Nested type, but try to unite flatten arrays to Nested type.
        if (!isNested(name_type.type) && type_arr)
        {
            auto split = splitName(name_type.name);
            if (!split.second.empty())
                nested[split.first].emplace_back(split.second, type_arr->getNestedType());
        }
    }

    std::map<String, DataTypePtr> nested_types;

    for (const auto & [name, elems] : nested)
        nested_types.emplace(name, createNested(elems.getTypes(), elems.getNames()));

    return nested_types;
}

}

NamesAndTypesList collect(const NamesAndTypesList & names_and_types)
{
    NamesAndTypesList res;
    auto nested_types = getSubcolumnsOfNested(names_and_types);

    for (const auto & name_type : names_and_types)
        if (!isArray(name_type.type) || !nested_types.contains(splitName(name_type.name).first))
            res.push_back(name_type);

    for (const auto & name_type : nested_types)
        res.emplace_back(name_type.first, name_type.second);

    return res;
}

NamesAndTypesList convertToSubcolumns(const NamesAndTypesList & names_and_types)
{
    auto nested_types = getSubcolumnsOfNested(names_and_types);
    auto res = names_and_types;

    for (auto & name_type : res)
    {
        if (!isArray(name_type.type))
            continue;

        auto split = splitName(name_type.name);
        if (name_type.isSubcolumn() || split.second.empty())
            continue;

        auto it = nested_types.find(split.first);
        if (it != nested_types.end())
            name_type = NameAndTypePair{split.first, split.second, it->second, it->second->getSubcolumnType(split.second)};
    }

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

            auto split = splitName(elem.name);

            /// Is it really a column of Nested data structure.
            if (!split.second.empty())
            {
                auto [it, inserted] = nested.emplace(split.first, i);

                /// It's not the first column of Nested data structure.
                if (!inserted)
                {
                    const ColumnArray & first_array_column = assert_cast<const ColumnArray &>(*block.getByPosition(it->second).column);
                    const ColumnArray & another_array_column = assert_cast<const ColumnArray &>(*elem.column);

                    if (!first_array_column.hasEqualOffsets(another_array_column))
                        throw Exception("Elements '" + block.getByPosition(it->second).name
                            + "' and '" + elem.name
                            + "' of Nested data structure '" + split.first
                            + "' (Array columns) have different array sizes.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
                }
            }
        }
    }
}


std::unordered_set<String> getAllTableNames(const Block & block, bool to_lower_case)
{
    std::unordered_set<String> nested_table_names;
    for (const auto & name : block.getNames())
    {
        auto nested_table_name = Nested::extractTableName(name);
        if (to_lower_case)
            boost::to_lower(nested_table_name);

        if (!nested_table_name.empty())
            nested_table_names.insert(std::move(nested_table_name));
    }
    return nested_table_names;
}

}

}
