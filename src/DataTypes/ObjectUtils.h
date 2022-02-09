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

size_t getNumberOfDimensions(const IDataType & type);
size_t getNumberOfDimensions(const IColumn & column);
DataTypePtr getBaseTypeOfArray(const DataTypePtr & type);
DataTypePtr createArrayOfType(DataTypePtr type, size_t num_dimensions);
Array createEmptyArrayField(size_t num_dimensions);

ColumnPtr getBaseColumnOfArray(const ColumnPtr & column);
ColumnPtr createArrayOfColumn(const ColumnPtr & column, size_t num_dimensions);

DataTypePtr getDataTypeByColumn(const IColumn & column);
void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block, const NamesAndTypesList & extended_storage_columns);
void checkObjectHasNoAmbiguosPaths(const PathsInData & paths);
DataTypePtr getLeastCommonTypeForObject(const DataTypes & types, bool check_ambiguos_paths = false);
void extendObjectColumns(NamesAndTypesList & columns_list, const ColumnsDescription & object_columns, bool with_subcolumns);

NameSet getNamesOfObjectColumns(const NamesAndTypesList & columns_list);
bool hasObjectColumns(const ColumnsDescription & columns);

void updateObjectColumns(ColumnsDescription & object_columns, const NamesAndTypesList & new_columns);

using DataTypeTuplePtr = std::shared_ptr<DataTypeTuple>;

std::pair<PathsInData, DataTypes> flattenTuple(const DataTypePtr & type);
ColumnPtr flattenTuple(const ColumnPtr & column);

DataTypePtr unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types);

std::pair<ColumnPtr, DataTypePtr> unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns);

void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query);

void finalizeObjectColumns(MutableColumns & columns);

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
        res.add({String(name), getLeastCommonTypeForObject(types)});

    return res;
}

}
