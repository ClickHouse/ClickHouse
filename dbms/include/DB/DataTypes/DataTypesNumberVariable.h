#ifndef DBMS_DATA_TYPES_NUMBER_VARIABLE_H
#define DBMS_DATA_TYPES_NUMBER_VARIABLE_H

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberVariable.h>


namespace DB
{

/** Типы столбцов для чисел переменной ширины. */

class DataTypeVarUInt : public IDataTypeNumberVariable<UInt64, ColumnUInt64>
{
public:
	std::string getName() const { return "VarUInt"; }
};

class DataTypeVarInt : public IDataTypeNumberVariable<Int64, ColumnInt64>
{
public:
	std::string getName() const { return "VarInt"; }
};

}

#endif
