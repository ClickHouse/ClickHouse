#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

DataTypePtr getDataTypeByColumn(const IColumn & column);
void convertObjectsToTuples(NamesAndTypesList & columns_list, Block & block);

}
