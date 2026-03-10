#pragma once

#include <DataTypes/DataTypeCustom.h>

namespace DB
{

class DataTypeSortedStringKV : public IDataTypeCustomName
{
public:
    String getName() const override { return "SortedStringKV"; }
};

}
