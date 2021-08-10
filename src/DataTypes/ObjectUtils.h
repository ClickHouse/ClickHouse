#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Common/FieldVisitors.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

size_t getNumberOfDimensions(const IDataType & type);
size_t getNumberOfDimensions(const IColumn & column);
DataTypePtr getBaseTypeOfArray(const DataTypePtr & type);
DataTypePtr createArrayOfType(DataTypePtr type, size_t dimension);

DataTypePtr getDataTypeByColumn(const IColumn & column);
void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block, const NamesAndTypesList & extended_storage_columns);
void checkObjectHasNoAmbiguosPaths(const Strings & key_names);
DataTypePtr getLeastCommonTypeForObject(const DataTypes & types);
NameSet getNamesOfObjectColumns(const NamesAndTypesList & columns_list);
void extendObjectColumns(NamesAndTypesList & columns_list, const ColumnsDescription & object_columns, bool with_subcolumns);

void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query);

void finalizeObjectColumns(MutableColumns & columns);

}
