#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

size_t getNumberOfDimensions(const IDataType & type);
size_t getNumberOfDimensions(const IColumn & column);
DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension);

DataTypePtr getDataTypeByColumn(const IColumn & column);
void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block);
DataTypePtr getLeastCommonTypeForObject(const DataTypes & types);
void optimizeTypesOfObjectColumns(MutableColumns & columns);

}
