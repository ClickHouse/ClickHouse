#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Common/FieldVisitors.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/JSONDataParser.h>

namespace DB
{

size_t getNumberOfDimensions(const IDataType & type);
size_t getNumberOfDimensions(const IColumn & column);
DataTypePtr getBaseTypeOfArray(const DataTypePtr & type);
DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension);

ColumnPtr getBaseColumnOfArray(const ColumnPtr & column);
ColumnPtr createArrayOfColumn(const ColumnPtr & column, size_t num_dimensions);

DataTypePtr getDataTypeByColumn(const IColumn & column);
void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block, const NamesAndTypesList & extended_storage_columns);
void checkObjectHasNoAmbiguosPaths(const Paths & paths);
DataTypePtr getLeastCommonTypeForObject(const DataTypes & types, bool check_ambiguos_paths = false);
NameSet getNamesOfObjectColumns(const NamesAndTypesList & columns_list);
void extendObjectColumns(NamesAndTypesList & columns_list, const ColumnsDescription & object_columns, bool with_subcolumns);

using DataTypeTuplePtr = std::shared_ptr<DataTypeTuple>;

std::pair<Paths, DataTypes> flattenTuple(const DataTypePtr & type);

DataTypePtr unflattenTuple(
    const Paths & paths,
    const DataTypes & tuple_types);

std::pair<DataTypePtr, ColumnPtr> unflattenTuple(
    const Paths & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns);

void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query);

void finalizeObjectColumns(MutableColumns & columns);

}
