#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Common/FieldVisitors.h>

namespace DB
{

using NameToTypeMap = std::unordered_map<String, DataTypePtr>;

size_t getNumberOfDimensions(const IDataType & type);
size_t getNumberOfDimensions(const IColumn & column);
DataTypePtr getBaseTypeOfArray(const DataTypePtr & type);
DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension);

DataTypePtr getDataTypeByColumn(const IColumn & column);
void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block, const NamesAndTypesList & extended_storage_columns);
DataTypePtr getLeastCommonTypeForObject(const DataTypes & types);
NameSet getNamesOfObjectColumns(const NamesAndTypesList & columns_list);
void extendObjectColumns(NamesAndTypesList & columns_list, const NameToTypeMap & object_types, bool with_subcolumns);

void finalizeObjectColumns(MutableColumns & columns);

}
