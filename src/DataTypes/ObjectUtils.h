#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Common/FieldVisitors.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnObject.h>

namespace DB
{

/// Returns number of dimensions in Array type. 0 if type is not array.
size_t getNumberOfDimensions(const IDataType & type);

/// Returns number of dimensions in Array column. 0 if column is not array.
size_t getNumberOfDimensions(const IColumn & column);

/// Returns type of scalars of Array of arbitrary dimensions.
DataTypePtr getBaseTypeOfArray(const DataTypePtr & type);

/// Returns Array type with requested scalar type and number of dimensions.
DataTypePtr createArrayOfType(DataTypePtr type, size_t num_dimensions);

/// Returns column of scalars of Array of arbitrary dimensions.
ColumnPtr getBaseColumnOfArray(const ColumnPtr & column);

/// Returns empty Array column with requested scalar column and number of dimensions.
ColumnPtr createArrayOfColumn(const ColumnPtr & column, size_t num_dimensions);

/// Returns Array with requested number of dimensions and no scalars.
Array createEmptyArrayField(size_t num_dimensions);

/// Tries to get data type by column. Only limited subset of types is supported
DataTypePtr getDataTypeByColumn(const IColumn & column);

/// Converts Object types and columns to Tuples in @columns_list and @block
/// and checks that types are consistent with types in @extended_storage_columns.
void convertObjectsToTuples(Block & block, const NamesAndTypesList & extended_storage_columns);

/// Checks that each path is not the prefix of any other path.
void checkObjectHasNoAmbiguosPaths(const PathsInData & paths);

/// Receives several Tuple types and deduces the least common type among them.
DataTypePtr getLeastCommonTypeForObject(const DataTypes & types, bool check_ambiguos_paths = false);

/// Converts types of object columns to tuples in @columns_list
/// according to @object_columns and adds all tuple's subcolumns if needed.
void extendObjectColumns(NamesAndTypesList & columns_list, const ColumnsDescription & object_columns, bool with_subcolumns);

NameSet getNamesOfObjectColumns(const NamesAndTypesList & columns_list);
bool hasObjectColumns(const ColumnsDescription & columns);
void finalizeObjectColumns(MutableColumns & columns);

/// Updates types of objects in @object_columns inplace
/// according to types in new_columns.
void updateObjectColumns(ColumnsDescription & object_columns, const NamesAndTypesList & new_columns);

using DataTypeTuplePtr = std::shared_ptr<DataTypeTuple>;

/// Flattens nested Tuple to plain Tuple. I.e extracts all paths and types from tuple.
/// E.g. Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64) -> Tuple(t.c1 UInt32, t.c2 String, c3 UInt32)
std::pair<PathsInData, DataTypes> flattenTuple(const DataTypePtr & type);

/// Flattens nested Tuple column to plain Tuple column.
ColumnPtr flattenTuple(const ColumnPtr & column);

/// The reverse operation to 'flattenTuple'.
/// Creates nested Tuple from all paths and types.
/// E.g. Tuple(t.c1 UInt32, t.c2 String, c3 UInt32) -> Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64)
DataTypePtr unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types);

std::pair<ColumnPtr, DataTypePtr> unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns);

/// For all columns which exist in @expected_columns and
/// don't exist in @available_columns adds to WITH clause
/// an alias with column name to literal of default value of column type.
void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query);

/// Receives range of objects, which contains collections
/// of columns-like objects (e.g. ColumnsDescription or NamesAndTypesList)
/// and deduces the common types of object columns for all entries.
/// @entry_columns_getter should extract reference to collection of
/// columns-like objects from entry to which Iterator points.
/// columns-like object should have fields "name" and "type".
template <typename Iterator, typename EntryColumnsGetter>
ColumnsDescription getObjectColumns(
    Iterator begin, Iterator end,
    const ColumnsDescription & storage_columns,
    EntryColumnsGetter && entry_columns_getter)
{
    ColumnsDescription res;

    if (begin == end)
    {
        for (const auto & column : storage_columns)
        {
            if (isObject(column.type))
            {
                auto tuple_type = std::make_shared<DataTypeTuple>(
                    DataTypes{std::make_shared<DataTypeUInt8>()},
                    Names{ColumnObject::COLUMN_NAME_DUMMY});

                res.add({column.name, std::move(tuple_type)});
            }
        }

        return res;
    }

    std::unordered_map<String, DataTypes> types_in_entries;

    for (auto it = begin; it != end; ++it)
    {
        const auto & entry_columns = entry_columns_getter(*it);
        for (const auto & column : entry_columns)
        {
            auto storage_column = storage_columns.tryGetPhysical(column.name);
            if (storage_column && isObject(storage_column->type))
                types_in_entries[column.name].push_back(column.type);
        }
    }

    for (const auto & [name, types] : types_in_entries)
        res.add({name, getLeastCommonTypeForObject(types)});

    return res;
}

}
